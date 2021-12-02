-module(blockchain_etf_decode).

-export([
    from_bin/1,
    binary_to_proplist/1
]).

-export_type([
    t/0
]).

-type t() :: term().

-spec from_bin(binary()) -> t().
from_bin(<<_/binary>>) ->
    error(not_implemented).

-spec binary_to_proplist(binary()) ->
    {ok, t()} | {error, Reason} when Reason :: any(). % TODO be more specific
binary_to_proplist(<<131, 108, Length:32/integer-unsigned-big, Rest/binary>>) ->
    case decode_list(Rest, Length, []) of
        {ok, {T, <<>>}} ->
            {ok, T};
        {error, _}=Err ->
            Err
    end.

-spec decode_list(binary(), integer(), list()) ->
    {ok, {[term()], binary()}} | {error, any()}.
decode_list(<<106, Rest/binary>>, 0, Acc) ->
    {ok, {lists:reverse(Acc), Rest}};
decode_list(Rest, 0, Acc) ->
    %% tuples don't end with an empty list
    {ok, {lists:reverse(Acc), Rest}};
decode_list(<<104, Size:8/integer, Bin/binary>>, Length, Acc) ->
    case decode_list(Bin, Size, []) of
        {ok, {List, Rest}} ->
            decode_list(Rest, Length - 1, [list_to_tuple(List)|Acc]);
        {error, _}=Err ->
            Err
    end;
decode_list(<<108, L2:32/integer-unsigned-big, Bin/binary>>, Length, Acc) ->
    case decode_list(Bin, L2, []) of
        {ok, {List, Rest}} ->
            decode_list(Rest, Length - 1, [List|Acc]);
        {error, _}=Err ->
            Err
    end;
decode_list(<<106, Rest/binary>>, Length, Acc) ->
    %% sometimes there's an embedded empty list
    decode_list(Rest, Length - 1, [[] |Acc]);
decode_list(Bin, Length, Acc) ->
    case decode_value(Bin) of
        {ok, {Val, Rest}} ->
            decode_list(Rest, Length -1, [Val|Acc]);
        {error, _}=Err ->
            Err
    end.

-spec decode_value(binary()) ->
    {ok, {term(), binary()}} | {error, any()}.
decode_value(<<97, Integer:8/integer, Rest/binary>>) ->
    {ok, {Integer, Rest}};
decode_value(<<98, Integer:32/integer-big, Rest/binary>>) ->
    {ok, {Integer, Rest}};
decode_value(<<100, AtomLen:16/integer-unsigned-big, Atom:AtomLen/binary, Rest/binary>>) ->
    {ok, {binary_to_atom(Atom, latin1), Rest}};
decode_value(<<109, Length:32/integer-unsigned-big, Bin:Length/binary, Rest/binary>>) ->
    {ok, {Bin, Rest}};
decode_value(<<110, N:8/integer, Sign:8/integer, Int:N/binary, Rest/binary>>) ->
    %% TODO Why bother calling decode_bigint before checking Sign?
    case decode_bigint(Int, 0, 0) of
        X when Sign == 0 ->
            {ok, {X, Rest}};
        X when Sign == 1 ->
            {ok, {X * -1, Rest}};
        X ->
            {error, {invalid_sign_for_bigint, Sign, X}}
    end;
decode_value(<<111, N:32/integer-unsigned-big, Sign:8/integer, Int:N/binary, Rest/binary>>) ->
    %% TODO Why bother calling decode_bigint before checking Sign?
    case decode_bigint(Int, 0, 0) of
        X when Sign == 0 ->
            {ok, {X, Rest}};
        X when Sign == 1 ->
            {ok, {X * -1, Rest}}
    end;
decode_value(<<116, Arity:32/integer-unsigned-big, MapAndRest/binary>>) ->
    decode_map(MapAndRest, Arity, #{});
%% TODO 107 - what is it? string?
decode_value(<<Code/integer, _Rest/binary>>) ->
    {error, {unknown_code, Code}}.

-spec decode_bigint(binary(), integer(), integer()) ->
    integer().
    %{ok, integer()} | {error, any()}.
decode_bigint(<<>>, _, Acc) ->
    Acc;
decode_bigint(<<B:8/integer, Rest/binary>>, Pos, Acc) ->
    decode_bigint(Rest, Pos + 1, Acc + (B bsl (8 * Pos))).

-spec decode_map(binary(), integer(), map()) ->
    {ok, {map(), binary()}} | {error, any()}.
decode_map(Rest, 0, Acc) ->
    {ok, {Acc, Rest}};
decode_map(Bin, Arity, Acc) ->
    case decode_value(Bin) of
        {ok, {Key, T1}} ->
            case decode_value(T1) of
                {ok, {Value, T2}} ->
                    decode_map(T2, Arity - 1, maps:put(Key, Value, Acc));
                {error, _}=Err ->
                    Err
            end;
        {error, _}=Err ->
            Err
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

nest(_, X, 0) -> X;
nest(F, X, N) -> nest(F, F(X), N - 1).

binary_to_proplist_test_() ->
    [
        ?_assertEqual({ok, Term}, binary_to_proplist(term_to_binary(Term)))
    ||
        Term <- [
            [{1, [<<>>]}],
            [{1, [<<"abcdefghijklmnopqrstuvwxyz">>]}],
            [{786587658765876587, [<<"abcdefghijklmnopqrstuvwxyz">>]}],
            [{k, v}],
            [{hello, goodbye}],
            [{foo, []}],
            [{}],
            nest(fun (X) -> [X] end, {}, 100),
            [[]],
            [[[]]],
            [[[[]]]],
            nest(fun (X) -> [X] end, [], 100),
            [{list, #{}}],
            [{list, #{1 => 2}}],
            [{foo, #{1 => 2}}],
            [{foo, #{bar => baz}}],
            [{foo, #{<<"bar">> => <<"baz">>}}]
        ]
    ]
    ++
    [
        {
            %% Adding a title because _assertMatch doesn't report values
            lists:flatten(io_lib:format("Term: ~p", [Term])),
            ?_assertMatch({error, _}, binary_to_proplist(term_to_binary(Term)))
        }
    ||
        Term <- [
            [{foo, #{"bar" => "baz"}}]
        ]
    ].

-endif.
