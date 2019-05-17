%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Channel Client ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_channel_client).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start/1,
    height/1,
    credits/1
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("blockchain.hrl").
-include("../pb/blockchain_data_credits_pb.hrl").

-define(SERVER, ?MODULE).

-record(state, {
    db :: rocksdb:db_handle(),
    cf :: rocksdb:cf_handle(),
    payer :: libp2p_crypto:pubkey_bin(),
    credits = 0 :: non_neg_integer(),
    height = 0 :: non_neg_integer(),
    pending = #{} :: #{binary() => non_neg_integer()}
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start(Args) ->
    gen_server:start(?SERVER, Args, []).

height(Pid) ->
    gen_statem:call(Pid, height).

credits(Pid) ->
    gen_statem:call(Pid, credits).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([DB, CF, Payer, Amount]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    self() ! {send_payment_req, Amount},
    {ok, #state{
        db=DB,
        cf=CF,
        payer=Payer
    }}.

handle_call(height, _From, #state{height=Height}=State) ->
    {reply, {ok, Height}, State};
handle_call(credits, _From, #state{credits=Credits}=State) ->
    {reply, {ok, Credits}, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info({send_payment_req, Amount}, #state{payer=Payer, pending=Pending0}=State) ->
    Swarm = blockchain_swarm:swarm(),
    P2PAddr = libp2p_crypto:pubkey_bin_to_p2p(Payer),
    case libp2p_swarm:dial_framed_stream(Swarm,
                                         P2PAddr,
                                         ?DATA_CREDITS_PAYMENT_PROTOCOL,
                                         blockchain_data_credits_payment_stream,
                                         [])
    of
        {ok, Stream} ->
            PubKeyBin = blockchain_swarm:pubkey_bin(),	
            PaymentReq = blockchain_data_credits_utils:new_payment_req(PubKeyBin, Amount),
            EncodedPaymentReq = blockchain_data_credits_utils:encode_payment_req(PaymentReq),
            lager:info("sending payment request (~p) to ~p", [Amount, Payer]),
            Stream ! {payment_req, EncodedPaymentReq},
            ID = PaymentReq#blockchain_data_credits_payment_req_pb.id,
            TimeRef = erlang:send_after(timer:seconds(60), self(), {payment_req_expired, PaymentReq}),
            Pending1 = maps:put(ID, TimeRef, Pending0),
            {noreply, State#state{pending=Pending1}};
        Error ->
            lager:error("failed to dial ~p ~p", [P2PAddr, Error]),
            {stop, dial_error, State}
    end;
handle_info({update, Payment}, #state{db=DB, cf=CF, height=Height,
                                      credits=Credits, pending=Pending0}=State) ->
    lager:info("got payment update ~p", [Payment]),
    Amount = Payment#blockchain_data_credits_payment_pb.amount,
    Payee = Payment#blockchain_data_credits_payment_pb.payee,
    Pending1 = case Payee == blockchain_swarm:pubkey_bin() of
        false ->
            Pending0;
        true ->
            ID = Payment#blockchain_data_credits_payment_pb.id,
            case maps:get(ID, Pending0, undefined) of
                undefined ->
                    Pending0; 
                TimeRef ->
                    _ = erlang:cancel_timer(TimeRef),
                    maps:remove(ID, Pending0)
            end
    end,
    ok = blockchain_data_credits_utils:store_payment(DB, CF, Payment),
    case Payment#blockchain_data_credits_payment_pb.height == 0 of
        true ->
            {noreply, State#state{height=0, credits=Amount, pending=Pending1}};
        false ->
            {noreply, State#state{height=Height+1, credits=Credits-Amount, pending=Pending1}}
    end;
handle_info({payment_req_expired, PaymentReq}, #state{payer=Payer, pending=Pending0}=State) ->
    ID = PaymentReq#blockchain_data_credits_payment_req_pb.id,
    Amount = PaymentReq#blockchain_data_credits_payment_req_pb.amount,
    lager:warning("Payment Req ~p for ~p (payer ~p) expired retrying...", [ID, Amount, Payer]),
    Pending1 = maps:remove(ID, Pending0),
    self() ! {send_payment_req, Amount},
    {noreply, State#state{pending=Pending1}};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------