%%%
%%% https://www.erlang.org/doc/apps/erts/erl_ext_dist.html
%%%
%%% TODO -spec next(State :: binary()) -> {ok, none | {some, {term(), State :: binary()}}} | {error, error()}

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
    | [t()]
    .

-type error() ::
      {malformed_envelope, binary()}
    | {unsupported_version, integer()}
    | {malformed_term, binary()}
    | {unsupported_tag, integer()}
    | {uncompressed_data_bad_size, {expected, integer(), actual, integer()}}
    | {malformed_list_ext, binary()}
    | {malformed_atom_ext, binary()}
    | {malformed_string_ext, binary()}
    | {malformed_big_int, binary()}
    | {malformed_big_int_sign, integer()}
    .

-define(TAG_TERM_COMPRESSED, 80).
-define(TAG_SMALL_INTEGER_EXT, 97).
-define(TAG_INTEGER_EXT, 98).
-define(TAG_ATOM_EXT, 100). % deprecated
-define(TAG_SMALL_TUPLE_EXT, 104).
-define(TAG_NIL_EXT, 106).
-define(TAG_STRING_EXT, 107).
-define(TAG_LIST_EXT, 108).
-define(TAG_BINARY_EXT, 109).
-define(TAG_SMALL_BIG_EXT, 110).

-spec from_bin(binary()) -> {ok, t()} | {error, error()}.
from_bin(<<Bin/binary>>) ->
    case envelope(Bin) of
        {ok, {Term, <<>>}} ->
            {ok, Term};
        {ok, {_, <<Bin/binary>>}} ->
            {error, {trailing_data_remains, Bin}};
        {error, _}=Err ->
            Err
    end.

%% 1       N
%% 131     Data
envelope(<<131, Data/binary>>) ->
    term(Data);
envelope(<<Version:8, _/binary>>) ->
    {error, {unsupported_version, Version}};
envelope(<<Bin/binary>>) ->
    {error, {malformed_envelope, Bin}}.

%% term:
%% -----
%% 1       N
%% Tag     Data
term(<<?TAG_TERM_COMPRESSED  , Rest/binary>>) -> term_compressed(Rest);
term(<<?TAG_SMALL_INTEGER_EXT, Rest/binary>>) -> small_integer_ext(Rest);
term(<<?TAG_INTEGER_EXT      , Rest/binary>>) -> integer_ext(Rest);
term(<<?TAG_ATOM_EXT         , Rest/binary>>) -> atom_ext(Rest);
term(<<?TAG_SMALL_TUPLE_EXT  , _/binary>>   ) -> {error, {not_implemented, 'SMALL_TUPLE_EXT'}}; % TODO
term(<<?TAG_NIL_EXT          , Rest/binary>>) -> {ok, {[], Rest}};
term(<<?TAG_STRING_EXT       , Rest/binary>>) -> string_ext(Rest);
term(<<?TAG_LIST_EXT         , Rest/binary>>) -> list_ext(Rest);
term(<<?TAG_BINARY_EXT       , _/binary>>   ) -> {error, {not_implemented, 'BINARY_EXT'}}; % TODO
term(<<?TAG_SMALL_BIG_EXT    , Rest/binary>>) -> small_big_ext(Rest);
term(<<Tag:8                 , _/binary>>   ) -> {error, {unsupported_tag, Tag}};
term(<<Bin/binary>>                         ) -> {error, {malformed_term, Bin}}.

%% SMALL_INTEGER_EXT
%% 1   1
%% 97  Int
small_integer_ext(<<Int/integer, Rest/binary>>) ->
    {ok, {Int, Rest}};
small_integer_ext(<<Bin/binary>>) ->
    {error, {malformed_small_integer_ext, Bin}}.

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
%% 1       4
%% 108     Length  Elements    Tail
list_ext(<<Len:32/integer-unsigned-big, Rest0/binary>>) ->
    case list_elements(Len, Rest0, []) of
        {ok, {Elements0, Rest1}} ->
            case term(Rest1) of
                {ok, {Tail, Rest2}} ->
                    Elements1 =
                        case Elements0 of
                            [] -> [];
                            [_|_] -> drop_tail_nil(Elements0 ++ [Tail])
                        end,
                    Term = Elements1,
                    Rest = Rest2,
                    {ok, {Term, Rest}};
                {error, _}=Err ->
                    Err
            end;
        {error, _}=Err ->
            Err
    end;
list_ext(<<Bin/binary>>) ->
    {error, {malformed_list_ext, Bin}}.

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
drop_tail_nil([X]) -> X;
drop_tail_nil([X | Xs]) -> [X | drop_tail_nil(Xs)].

%% ATOM_EXT (deprecated):
%% ---------------------
%% 1 	2 	    Len
%% 100 	Len 	AtomName
%%
%% An atom is stored with a 2 byte unsigned length in big-endian order,
%% followed by Len numbers of 8-bit Latin-1 characters that forms the AtomName.
%% The maximum allowed value for Len is 255.
atom_ext(<<Len:16, AtomName:Len/binary, Rest/binary>>) ->
    Term = binary_to_atom(AtomName),
    {ok, {Term, Rest}};
atom_ext(<<Bin/binary>>) ->
    {error, {malformed_atom_ext, Bin}}.


%% STRING_EXT
%% 1       2       Len
%% 107     Length  Characters
string_ext(<<Len:16, Characters:Len/binary, Rest/binary>>) ->
    Term = binary_to_list(Characters),
    {ok, {Term, Rest}};
string_ext(<<Bin/binary>>) ->
    {error, {malformed_string_ext, Bin}}.

small_big_ext(<<N:8, Sign:8, Data:N/binary, Rest/binary>>) ->
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
    end;
small_big_ext(<<Bin/binary>>) ->
    {error, {malformed_big_int, Bin}}.

big_int_sign_to_multiplier(0) -> {ok,  1};
big_int_sign_to_multiplier(1) -> {ok, -1};
big_int_sign_to_multiplier(S) -> {error, {malformed_big_int_sign, S}}.

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
            lists:flatten(io_lib:format("Opts:~p, Term:~p", [Opts, Term])),
            ?_assertEqual({ok, Term}, from_bin(term_to_binary(Term, Opts)))
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
            [1000, 2000, 3000, 4000 | improper_list_tail]
        ],
        Opts <- [[], [compressed]]
    ].

-endif.
