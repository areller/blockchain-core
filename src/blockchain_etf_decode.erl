-module(blockchain_etf_decode).

-export([
    from_bin/1
]).

-export_type([
    t/0
]).

-type t() :: term().

-define(VERSION, 131).

-define(TAG_SMALL_INTEGER_EXT, 97).
-define(TAG_INTEGER_EXT, 98).
-define(TAG_ATOM_EXT, 100). % deprecated
-define(TAG_SMALL_TUPLE_EXT, 104).
-define(TAG_NIL_EXT, 106).
-define(TAG_LIST_EXT, 108).
-define(TAG_BINARY_EXT, 109).
-define(TAG_SMALL_BIG_EXT, 110).
-define(TAG_LARGE_BIG_EXT, 111).
-define(TAG_MAP_EXT, 116).

-spec from_bin(binary()) -> t().
from_bin(<<Bin/binary>>) ->
    envelope(Bin).

envelope(<<?VERSION, Data/binary>>) ->
    term(Data);
envelope(<<Version:8, _/binary>>) ->
    {error, {unsupported_version, Version}};
envelope(<<Bin/binary>>) ->
    {error, {malformed_envelope, Bin}}.

term(<<?TAG_LIST_EXT, Data/binary>>) -> list_ext(Data);
term(<<Tag:8, _/binary>>) -> {error, {unsupported_tag, Tag}};
term(<<Bin/binary>>) -> {error, {malformed_term, Bin}}.

-spec list_ext(binary()) ->
    {ok, t()} | {error, Reason} when Reason :: any(). % TODO be more specific
list_ext(<<Len:32/integer-unsigned-big, Data/binary>>) ->
    case decode_list(Data, Len, []) of
        {ok, {T, <<>>}} ->
            {ok, T};
        {error, _}=Err ->
            Err
    end.

-spec decode_list(binary(), integer(), list()) ->
    {ok, {[term()], binary()}} | {error, any()}.
decode_list(<<?TAG_NIL_EXT, Rest/binary>>, 0, Acc) ->
    {ok, {lists:reverse(Acc), Rest}};
decode_list(Rest, 0, Acc) ->
    %% tuples don't end with an empty list
    {ok, {lists:reverse(Acc), Rest}};
decode_list(<<?TAG_SMALL_TUPLE_EXT, Size:8/integer, Bin/binary>>, Length, Acc) ->
    case decode_list(Bin, Size, []) of
        {ok, {List, Rest}} ->
            decode_list(Rest, Length - 1, [list_to_tuple(List)|Acc]);
        {error, _}=Err ->
            Err
    end;
decode_list(<<?TAG_LIST_EXT, L2:32/integer-unsigned-big, Bin/binary>>, Length, Acc) ->
    case decode_list(Bin, L2, []) of
        {ok, {List, Rest}} ->
            decode_list(Rest, Length - 1, [List|Acc]);
        {error, _}=Err ->
            Err
    end;
decode_list(<<?TAG_NIL_EXT, Rest/binary>>, Length, Acc) ->
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
decode_value(<<?TAG_SMALL_INTEGER_EXT, Integer:8/integer, Rest/binary>>) ->
    {ok, {Integer, Rest}};
decode_value(<<?TAG_INTEGER_EXT, Integer:32/integer-big, Rest/binary>>) ->
    {ok, {Integer, Rest}};
decode_value(<<?TAG_ATOM_EXT, AtomLen:16/integer-unsigned-big, Atom:AtomLen/binary, Rest/binary>>) ->
    {ok, {binary_to_atom(Atom, latin1), Rest}};
decode_value(<<?TAG_BINARY_EXT, Length:32/integer-unsigned-big, Bin:Length/binary, Rest/binary>>) ->
    {ok, {Bin, Rest}};
decode_value(<<?TAG_SMALL_BIG_EXT, N:8/integer, Sign:8/integer, Int:N/binary, Rest/binary>>) ->
    %% TODO Why bother calling decode_bigint before checking Sign?
    case decode_bigint(Int, 0, 0) of
        X when Sign == 0 ->
            {ok, {X, Rest}};
        X when Sign == 1 ->
            {ok, {X * -1, Rest}};
        X ->
            {error, {invalid_sign_for_bigint, Sign, X}}
    end;
decode_value(<<?TAG_LARGE_BIG_EXT, N:32/integer-unsigned-big, Sign:8/integer, Int:N/binary, Rest/binary>>) ->
    %% TODO Why bother calling decode_bigint before checking Sign?
    case decode_bigint(Int, 0, 0) of
        X when Sign == 0 ->
            {ok, {X, Rest}};
        X when Sign == 1 ->
            {ok, {X * -1, Rest}}
    end;
decode_value(<<?TAG_MAP_EXT, Arity:32/integer-unsigned-big, MapAndRest/binary>>) ->
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
        [
            {
                lists:flatten(io_lib:format("OLD. Term: ~p", [Term])),
                ?_assertEqual({ok, Term}, from_bin(term_to_binary(Term)))
            },
            {
                lists:flatten(io_lib:format("NEW. Term: ~p", [Term])),
                ?_assertEqual({ok, Term}, blockchain_term:from_bin(term_to_binary(Term)))
            }
        ]
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
            [{foo, #{<<"bar">> => <<"baz">>}}],
            [{k, [{k, [{k, [{k, v}]}]}]}],
            [{k, [{k, [{k, [{k, #{k => v}}]}]}]}],
            [{k, [{k, [{k, [{k, #{k => #{}}}]}]}]}],
            [{k, #{k1 => #{k2 => #{k3 => hi}}}}],
            [{1, 2, 3, 4, 5}]
        ]
    ]
    ++
    [
        %% Title is especially important with _assertMatch, because
        %% it doesn't report values:
        [
            {
                lists:flatten(io_lib:format("OLD. Term: ~p", [Term])),
                ?_assertMatch({error, _}, from_bin(term_to_binary(Term)))
            },
            {
                lists:flatten(io_lib:format("NEW. Term: ~p", [Term])),
                ?_assertEqual({ok, Term}, blockchain_term:from_bin(term_to_binary(Term)))
            }
        ]
    ||
        Term <- [
            1,
            {1, 2, 3, 4, 5},
            list_to_tuple(lists:seq(1, 1000)),
            [list_to_tuple(lists:seq(1, 1000))],
            #{k1 => #{k2 => #{k3 => hi}}},
            #{k => v},
            #{},
            [],

            % strings unsupported
            "",
            [{foo, #{"bar" => "baz"}}]
        ]
    ].

-endif.
