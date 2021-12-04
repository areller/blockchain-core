%%%
%%% https://www.erlang.org/doc/apps/erts/erl_ext_dist.html
%%%

-module(blockchain_term).

-export([
    from_bin/1
]).

-export_type([
    t/0
]).

-type t() ::
      integer()
    | atom()
    | string()
    | binary()
    | tuple()
    | [t()]
    | nonempty_improper_list(t(), t())
    | #{t() => t()}
    .

-type error() ::
      {trailing_data_remains, binary()}
    | {malformed_envelope, binary()}
    | {unsupported_version, integer()}
    | {malformed_term, binary()}
    | {unsupported_tag, integer()}
    | {uncompressed_data_bad_size, {expected, integer(), actual, integer()}}
    | {malformed_list, empty_with_non_nil_tail}
    | {malformed_list_ext, binary()}
    | {malformed_atom_ext, binary()}
    | {malformed_string_ext, binary()}
    | {malformed_small_big_int, binary()}
    | {malformed_large_big_int, binary()}
    | {malformed_big_int_sign, integer()}
    .

-define(VERSION, 131).

-define(TAG_TERM_COMPRESSED  , 80).
-define(TAG_SMALL_INTEGER_EXT, 97).
-define(TAG_INTEGER_EXT      , 98).
-define(TAG_ATOM_EXT         , 100). % deprecated
-define(TAG_SMALL_TUPLE_EXT  , 104).
-define(TAG_LARGE_TUPLE_EXT  , 105).
-define(TAG_NIL_EXT          , 106).
-define(TAG_STRING_EXT       , 107).
-define(TAG_LIST_EXT         , 108).
-define(TAG_BINARY_EXT       , 109).
-define(TAG_SMALL_BIG_EXT    , 110).
-define(TAG_LARGE_BIG_EXT    , 111).
-define(TAG_MAP_EXT          , 116).

-spec from_bin(binary()) -> {ok, t()} | {error, error()}.
from_bin(<<Bin/binary>>) ->
    case envelope(Bin) of
        {ok, {Term, <<>>}} ->
            {ok, Term};
        {ok, {_, <<Rest/binary>>}} ->
            %% TODO Would it make any sense to return OK here?
            %%      Let's say if we concatenated multiple t2b outputs.
            {error, {trailing_data_remains, Rest}};
        {error, _}=Err ->
            Err
    end.

%% 1       N
%% 131     Data
-spec envelope(binary()) -> {ok, {t(), binary()}} | {error, error()}.
envelope(<<?VERSION, Data/binary>>) ->
    term(Data);
envelope(<<Version:8, _/binary>>) ->
    {error, {unsupported_version, Version}};
envelope(<<Bin/binary>>) ->
    {error, {malformed_envelope, Bin}}.

%% term:
%% -----
%% 1       N
%% Tag     Data
-spec term(binary()) -> {ok, {t(), binary()}} | {error, error()}.
term(<<?TAG_TERM_COMPRESSED  , Rest/binary>>) -> term_compressed(Rest);
term(<<?TAG_SMALL_INTEGER_EXT, Rest/binary>>) -> small_integer_ext(Rest);
term(<<?TAG_INTEGER_EXT      , Rest/binary>>) -> integer_ext(Rest);
term(<<?TAG_ATOM_EXT         , Rest/binary>>) -> atom_ext(Rest);
term(<<?TAG_SMALL_TUPLE_EXT  , Rest/binary>>) -> small_tuple_ext(Rest);
term(<<?TAG_LARGE_TUPLE_EXT  , Rest/binary>>) -> large_tuple_ext(Rest);
term(<<?TAG_NIL_EXT          , Rest/binary>>) -> {ok, {[], Rest}};
term(<<?TAG_STRING_EXT       , Rest/binary>>) -> string_ext(Rest);
term(<<?TAG_LIST_EXT         , Rest/binary>>) -> list_ext(Rest);
term(<<?TAG_BINARY_EXT       , Rest/binary>>) -> binary_ext(Rest);
term(<<?TAG_SMALL_BIG_EXT    , Rest/binary>>) -> small_big_ext(Rest);
term(<<?TAG_LARGE_BIG_EXT    , Rest/binary>>) -> large_big_ext(Rest);
term(<<?TAG_MAP_EXT          , Rest/binary>>) -> map_ext(Rest);
term(<<Tag:8                 , _/binary>>   ) -> {error, {unsupported_tag, Tag}};
term(<<Bin/binary>>                         ) -> {error, {malformed_term, Bin}}.

-spec binary_ext(binary()) ->
    {ok, {binary(), binary()}} | {error, {malformed_binary_ext, binary()}}.
binary_ext(<<Len:32, Bin:Len/binary, Rest/binary>>) ->
    {ok, {Bin, Rest}};
binary_ext(<<Bin/binary>>) ->
    {error, {malformed_binary_ext, Bin}}.

%% MAP_EXT
%% 4       N
%% Arity   Pairs
%%
%% Encodes a map. The Arity field is an unsigned 4 byte integer in big-endian
%% format that determines the number of key-value pairs in the map. Key and
%% value pairs (Ki => Vi) are encoded in section Pairs in the following order:
%% K1, V1, K2, V2,..., Kn, Vn. Duplicate keys are not allowed within the same
%% map.
-spec map_ext(binary()) -> {ok, {#{t() => t()}, binary()}} | {error, error()}.
map_ext(<<Arity:32/integer-unsigned-big, Rest0/binary>>) ->
    case map_pairs(Arity, [], Rest0) of
        {ok, {Pairs, Rest1}} ->
            Term = maps:from_list(Pairs), % TODO Handle errors?
            {ok, {Term, Rest1}};
        {error, _}=Err ->
            Err
    end;
map_ext(<<Bin/binary>>) ->
    {error, {malformed_map, Bin}}.

-spec map_pairs(non_neg_integer(), [{t(), t()}], binary()) ->
    {ok, {[{t(), t()}], binary()}} | {error, error()}.
map_pairs(0, Pairs, <<Rest/binary>>) ->
    {ok, {Pairs, Rest}};
map_pairs(N, Pairs, <<Rest0/binary>>) ->
    case term(Rest0) of
        {ok, {Key, Rest1}} ->
            case term(Rest1) of
                {ok, {Val, Rest2}} ->
                    map_pairs(N - 1, [{Key, Val} | Pairs], Rest2);
                {error, _}=Err ->
                    Err
            end;
        {error, _}=Err ->
            Err
    end.

%% SMALL_INTEGER_EXT
%% 1
%% Int
-spec small_integer_ext(binary()) ->
    {ok, {integer(), binary()}} | {error, {malformed_small_integer_ext, binary()}}.
small_integer_ext(<<Int/integer, Rest/binary>>) ->
    {ok, {Int, Rest}};
small_integer_ext(<<Bin/binary>>) ->
    {error, {malformed_small_integer_ext, Bin}}.


%% INTEGER_EXT
%% 4
%% Int
%%
%% Signed 32-bit integer in big-endian format.
-spec integer_ext(binary()) ->
    {ok, {integer(), binary()}} | {error, {malformed_small_integer_ext, binary()}}.
integer_ext(<<Int:32/integer-big, Rest/binary>>) ->
    {ok, {Int, Rest}};
integer_ext(<<Bin/binary>>) ->
    {error, {malformed_integer_ext, Bin}}.

%% compressed term:
%% ----------------------------------------
%% 4                   N
%% UncompressedSize    Zlib-compressedData
%%
%% Uncompressed size (unsigned 32-bit integer in big-endian byte order) is the
%% size of the data before it was compressed. The compressed data has the
%% following format when it has been expanded:
%% 1    Uncompressed Size
%% Tag  Data
%%
-spec term_compressed(binary()) -> {ok, {t(), binary()}} | {error, error()}.
term_compressed(<<UncompressedSize:32/integer-unsigned-big, ZlibCompressedData/binary>>) ->
    % TODO More kinds of errors? Exceptions?
    case zlib:uncompress(ZlibCompressedData) of
        <<Data:UncompressedSize/binary>> ->
            term(Data);
        <<Data/binary>> ->
            {error,
                {uncompressed_data_bad_size,
                    {expected, UncompressedSize, actual, bit_size(Data)}}} % TODO or byte?
    end.

%% LIST_EXT
%% 4
%% Length  Elements    Tail
-spec list_ext(binary()) ->
    {ok, {maybe_improper_list(t(), t()), binary()}} | {error, error()}.
list_ext(<<Len:32/integer-unsigned-big, Rest0/binary>>) ->
    case list_elements(Len, Rest0, []) of
        {ok, {Elements, Rest1}} ->
            case term(Rest1) of
                {ok, {Tail, Rest2}} ->
                    R = Rest2,
                    case {Elements, Tail} of
                        {[], []} -> {ok, {[], R}};
                        {_ , []} -> {ok, {Elements, R}};
                        {[], _ } -> {error, {malformed_list, empty_with_non_nil_tail}};
                        {_ , _ } -> {ok, {list_improper(Elements ++ [Tail]), R}}
                    end;
                {error, _}=Err ->
                    Err
            end;
        {error, _}=Err ->
            Err
    end;
list_ext(<<Bin/binary>>) ->
    {error, {malformed_list_ext, Bin}}.

-spec list_elements(non_neg_integer(), binary(), [t()]) ->
    {ok, {[t()], binary()}} | {error, error()}.
list_elements(0, <<Rest/binary>>, Xs) ->
    {ok, {lists:reverse(Xs), Rest}};
list_elements(N, <<Rest0/binary>>, Xs) ->
    case term(Rest0) of
        {ok, {X, Rest1}} ->
            list_elements(N - 1, Rest1, [X | Xs]);
        {error, _}=Err ->
            Err
    end.

%% XXX Caller must ensure at least 2 elements!
-spec list_improper(nonempty_list(A | B)) -> nonempty_improper_list(A, B) | A | B.
list_improper([X]) -> X;
list_improper([X | Xs]) -> [X | list_improper(Xs)].

%% SMALL_TUPLE_EXT
%% 1       N
%% Arity   Elements
%%
%% Encodes a tuple. The Arity field is an unsigned byte that determines how
%% many elements that follows in section Elements.
-spec small_tuple_ext(binary()) ->
    {ok, {tuple(), binary()}} | {error, error()}.
small_tuple_ext(<<Arity:8/integer-unsigned, Rest/binary>>) ->
    tuple_ext(Arity, Rest);
small_tuple_ext(<<Bin/binary>>) ->
    {error, {malformed_small_tuple_ext, Bin}}.

%% LARGE_TUPLE_EXT
%% 4       N
%% Arity   Elements
%%
%% Same as SMALL_TUPLE_EXT except that Arity is an unsigned 4 byte integer in
%% big-endian format.
-spec large_tuple_ext(binary()) ->
    {ok, {tuple(), binary()}} | {error, error()}.
large_tuple_ext(<<Arity:32/integer-unsigned-big, Rest/binary>>) ->
    tuple_ext(Arity, Rest);
large_tuple_ext(<<Bin/binary>>) ->
    {error, {malformed_large_tuple_ext, Bin}}.

-spec tuple_ext(non_neg_integer(), binary()) ->
    {ok, {tuple(), binary()}} | {error, error()}.
tuple_ext(Arity, <<Rest0/binary>>) ->
    case list_elements(Arity, Rest0, []) of
        {ok, {Elements, Rest1}} ->
            Term = list_to_tuple(Elements),
            Rest = Rest1,
            {ok, {Term, Rest}};
        {error, _}=Err ->
            Err
    end.

%% ATOM_EXT (deprecated):
%% ---------------------
%% 2 	Len
%% Len 	AtomName
%%
%% An atom is stored with a 2 byte unsigned length in big-endian order,
%% followed by Len numbers of 8-bit Latin-1 characters that forms the AtomName.
%% The maximum allowed value for Len is 255.
-spec atom_ext(binary()) ->
    {ok, {atom(), binary()}} | {error, {malformed_atom_ext, binary()}}.
atom_ext(<<Len:16/integer-unsigned-big, AtomName:Len/binary, Rest/binary>>) ->
    Term = binary_to_atom(AtomName),
    {ok, {Term, Rest}};
atom_ext(<<Bin/binary>>) ->
    {error, {malformed_atom_ext, Bin}}.


%% STRING_EXT
%% 2       Len
%% Length  Characters
%%
%% String does not have a corresponding Erlang representation, but is an
%% optimization for sending lists of bytes (integer in the range 0-255) more
%% efficiently over the distribution. As field Length is an unsigned 2 byte
%% integer (big-endian), implementations must ensure that lists longer than
%% 65535 elements are encoded as LIST_EXT.
-spec string_ext(binary()) ->
    {ok, {string(), binary()}} | {error, {malformed_string_ext, binary()}}.
string_ext(<<Len:16/integer-unsigned-big, Characters:Len/binary, Rest/binary>>) ->
    Term = binary_to_list(Characters),
    {ok, {Term, Rest}};
string_ext(<<Bin/binary>>) ->
    {error, {malformed_string_ext, Bin}}.


%% SMALL_BIG_EXT
%% 1   1       n
%% n   Sign    d(0) ... d(n-1)
%%
%% Bignums are stored in unary form with a Sign byte, that is, 0 if the bignum
%% is positive and 1 if it is negative. The digits are stored with the least
%% significant byte stored first. To calculate the integer, the following
%% formula can be used:
%%
%% B = 256
%% (d0*B^0 + d1*B^1 + d2*B^2 + ... d(N-1)*B^(n-1))
-spec small_big_ext(binary()) ->
    {ok, {integer(), binary()}} | {error, {malformed_small_big_int, binary()}}.
small_big_ext(<<N:8, Sign:8, Data:N/binary, Rest/binary>>) ->
    big_ext(Sign, Data, <<Rest/binary>>);
small_big_ext(<<Bin/binary>>) ->
    {error, {malformed_small_big_int, Bin}}.

%% LARGE_BIG_EXT
%% 4   1       n
%% n   Sign    d(0) ... d(n-1)
%%
%% Same as SMALL_BIG_EXT except that the length field is an unsigned 4 byte
%% integer.
-spec large_big_ext(binary()) ->
    {ok, {integer(), binary()}} | {error, error()}.
large_big_ext(<<N:32, Sign:8, Data:N/binary, Rest/binary>>) ->
    big_ext(Sign, Data, <<Rest/binary>>);
large_big_ext(<<Bin/binary>>) ->
    {error, {malformed_large_big_int, Bin}}.

-spec big_ext(0 | 1, binary(), binary()) ->
    {ok, {integer(), binary()}} | {error, error()}.
big_ext(Sign, <<Data/binary>>, <<Rest/binary>>) ->
    case big_int_sign_to_multiplier(Sign) of
        {ok, Multiplier} ->
            case big_int_data(Data, 0, 0) of
                {ok, Int} ->
                    Term = Int * Multiplier,
                    {ok, {Term, Rest}};
                {error, _}=Err ->
                    Err
            end;
        {error, _}=Err ->
            Err
    end.

-spec big_int_sign_to_multiplier(integer()) ->
    {ok,  1 | -1} | {error, {malformed_big_int_sign, integer()}}.
big_int_sign_to_multiplier(0) -> {ok,  1};
big_int_sign_to_multiplier(1) -> {ok, -1};
big_int_sign_to_multiplier(S) -> {error, {malformed_big_int_sign, S}}.

-spec big_int_data(binary(), integer(), integer()) ->
    {ok, integer()} | {error, {malformed_big_int_data, binary()}}.
big_int_data(<<>>, _, Int) ->
    {ok, Int};
big_int_data(<<B:8/integer, Rest/binary>>, Pos, Int) ->
    big_int_data(Rest, Pos + 1, Int + (B bsl (8 * Pos)));
big_int_data(<<Bin/binary>>, _, _) ->
    {error, {malformed_big_int_data, Bin}}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

term_test_() ->
    [
        {
            lists:flatten(io_lib:format("Opts:~p Term: ~p", [Opts, Term])),
            ?_assertEqual({ok, Term}, from_bin(term_to_binary(Term)))
        }
    ||
        Term <- [
            a,
            b,
            ab,
            abcdefghijklmnopqrstuvwxyz,
            aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,
            [],
            "",
            0,
            "a",
            "abcdefghijklmnopqrstuvwxyz",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            lists:seq(0, 100),
            lists:seq(0, 1000),
            [500],
            9999999999999999999999999999999999999999999999999999999999999999999,
            -9999999999999999999999999999999999999999999999999999999999999999999,
            [a | b],
            [1000, 2000, 3000, 4000 | improper_list_tail],
            [foo],
            ["foo"],

            %% TODO A non-hand-wavy LARGE_BIG_EXT
            ceil(math:pow(10, 300)) * ceil(math:pow(10, 300)) * ceil(math:pow(10, 300)),

            {},
            {1, 2, 3},
            [{}],
            [{k, v}],
            [{"k", "v"}],
            [<<"foo">>],
            [{<<"k">>, <<"v">>}],
            #{a => 1},
            #{1 => a},
            #{0 => 1},
            #{k => v},
            #{"k" => "v"},
            #{<<"k">> => <<"v">>},
            #{k1 => #{k2 => #{k3 => hi}}}
        ],
        Opts <- [[], [compressed]]
    ].

-endif.
