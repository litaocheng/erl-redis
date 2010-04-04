%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng@gmail.com
%%% @doc the redis protocol module
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis_proto).
-author('ltiaocheng@gmail.com').
-vsn('0.1').
-include("redis_internal.hrl").

-export([line/1, line/2, line/3, line/4, line_list/1,
         bulk/3, bulk/4, mbulk/1]).
-export([parse_reply/2]).
-export([tokens/2]).

-compile([opt_bin_info]).
-compile({inline, [line/1, line/2, line/3, line/4, line_list/1,
         bulk/3, bulk/4, mbulk/1, parse_reply/2]}).  

%% @doc generate the line 
-spec line(iolist()) -> iolist().
line(Type) ->
    [Type, ?CRLF].

-spec line(iolist(), iolist()) ->
    iolist().
line(Type, Arg) ->
    [Type, ?SEP, Arg, ?CRLF].

-spec line(iolist(), iolist(), iolist()) -> 
    iolist().
line(Type, Arg1, Arg2) ->
    [Type, ?SEP, Arg1, ?SEP, Arg2, ?CRLF].

-spec line(iolist(), iolist(), iolist(), iolist()) -> 
    iolist().
line(Type, Arg1, Arg2, Arg3) ->
    [Type, ?SEP, Arg1, ?SEP, Arg2, ?SEP, Arg3, ?CRLF].

-spec line_list([iolist()]) ->
    iolist().
line_list(Parts) ->
    [?SEP | Line] = 
    lists:foldr(
        fun(P, Acc) ->
            [?SEP, P | Acc]
        end,
    [?CRLF], Parts),
    Line.

%% @doc generate the bulk command
-spec bulk(iolist(), iolist(), iolist()) -> 
    iolist().
bulk(Type, Arg1, Arg2) ->
    L1 = line(Type, Arg1, ?N2S(iolist_size(Arg2))),
    L2 = line(Arg2),
    [L1, L2].
-spec bulk(iolist(), iolist(), iolist(), iolist()) -> 
    iolist().
bulk(Type, Arg1, Arg2, Arg3) ->
    L1 = line(Type, Arg1, Arg2, ?N2S(iolist_size(Arg3))),
    L2 = line(Arg3),
    [L1, L2].

%% @doc generate the mbulk command
-spec mbulk(L :: [iolist()]) ->
    iolist().
mbulk(L) ->
    N = length(L),
    Lines = [mbulk1(E) || E <- L],
    ["*", ?N2S(N), ?CRLF | Lines].

%% @doc parse the reply
-spec parse_reply(Bin :: binary(), any()) ->
    any().
parse_reply(<<"+", Rest/binary>>, init) ->
    parse_status_reply(Rest);
parse_reply(<<"-", Rest/binary>>, init) ->
    parse_error_reply(Rest);
parse_reply(<<":", Rest/binary>>, init) ->
    b2n(Rest);

parse_reply(<<"$-1\r\n">>, init) ->
    null;
parse_reply(<<"$0\r\n">>, init) ->
    <<>>;
parse_reply(<<"$", Rest/binary>>, init) ->
    N = b2n(Rest),
    {bulk_more, N};
parse_reply(Bin, {bulk_more, N}) ->
    <<Val:N/bytes, "\r\n">> = Bin,
    Val;

parse_reply(<<"*-1\r\n">>, init) ->
    null;
parse_reply(<<"*0\r\n">>, init) ->
    null;
parse_reply(<<"*", Rest/binary>>, init) ->
    N = b2n(Rest),
    {mbulk_more, N, [], next};
parse_reply(Bin, {mbulk_more, N, Acc, Bulk = {bulk_more, _}}) ->
    Item = parse_reply(Bin, Bulk),
    ?DEBUG2("parse_reply n:~p item:~p", [N, Item]),
    case N of
        1 ->
            lists:reverse([Item | Acc]);
        _ ->
            {mbulk_more, N-1, [Item | Acc], next}
    end;
parse_reply(Bin, {mbulk_more, N, Acc, next}) ->
    Bulk = parse_reply(Bin, init),
    ?DEBUG2("parse_reply n:~p bulk:~p", [N, Bulk]),
    case Bulk of
        null ->
            case N of
                1 ->
                    lists:reverse([null | Acc]);
                _ ->
                    {mbulk_more, N-1, [null | Acc], next}
            end;
        _ ->
            {mbulk_more, N, Acc, Bulk}
    end.

%% @doc return a list of tokens in string, separated by the characters
%%  in Separatorlist
-spec tokens(S :: binary(), Sep :: char()) -> [binary()].
tokens(S, Sep) when is_integer(Sep) ->
    ?DEBUG2("string is ~p sep is ~p", [S, Sep]),
    tokens1(S, Sep, []).

%%
%%------------------------------------------------------------------------------
%%
%% internal API
%%
%%------------------------------------------------------------------------------

%% generte mbulk command line
mbulk1(B) when is_binary(B) ->
    N = byte_size(B),
    ["$", ?N2S(N), ?CRLF, B, ?CRLF];
mbulk1(L) when is_list(L) ->
    N = length(L),
    ["$", ?N2S(N), ?CRLF, L, ?CRLF].

%% tokens
tokens1(<<C, Rest/binary>>, C, Toks) ->
    tokens1(Rest, C, Toks);
tokens1(<<C, Rest/binary>>, Sep, Toks) ->
    tokens2(Rest, Sep, Toks, <<C>>);
tokens1(<<>>, _Sep, Toks) ->
    lists:reverse(Toks).

tokens2(<<C, Rest/binary>>, C, Toks, Bin) ->
    tokens1(Rest, C, [Bin | Toks]); 
tokens2(<<C, Rest/binary>>, Sep, Toks, Bin) ->
    tokens2(Rest, Sep, Toks, <<Bin/binary, C>>);
tokens2(<<>>, _Sep, Toks, Bin) ->
    lists:reverse([Bin | Toks]).

%% parse status reply
parse_status_reply(<<"OK\r\n">>) ->
    ok;
parse_status_reply(<<"QUEUED\r\n">>) ->
    queued;
parse_status_reply(<<"PONG\r\n">>) ->
    pong;
parse_status_reply(<<"none\r\n">>) ->
    none;
parse_status_reply(<<"string\r\n">>) ->
    string;
parse_status_reply(<<"list\r\n">>) ->
    list;
parse_status_reply(<<"set\r\n">>) ->
    set;
parse_status_reply(Status) ->
    Len = byte_size(Status) - 2,
    <<Val:Len/bytes, "\r\n">> = Status,
    Val.

%% parse error reply
parse_error_reply(Bin) when is_binary(Bin) ->
    L = byte_size(Bin) - 2,
    <<Msg:L/bytes, "\r\n">> = Bin,
    {error, Msg}.

%% binary to integer
b2n(<<"0\r\n">>) ->
    0;
b2n(<<"1\r\n">>) ->
    1;
b2n(<<$-, Rest/binary>>) ->
    -b2n(Rest);
b2n(Bin) ->
    b2n(Bin, 0).

b2n(<<C, Rest/binary>>, N) when C >= $0, C =< $9 ->
    b2n(Rest, N * 10 + (C - $0));
b2n(<<"\r\n">>, N) ->
    N.

-ifdef(TEST).

b2n_test_() ->
    [
        ?_assertEqual(233, b2n(<<"233\r\n">>)),     
        ?_assertEqual(0, b2n(<<"0\r\n">>)),     
        ?_assertEqual(123, b2n(<<"123\r\n">>)),     
        ?_assertEqual(12432, b2n(<<"12432\r\n">>))     
    ].

parse_reply(Bin) ->
    parse_reply(Bin, init).

parse_test() ->
    ?assertEqual(ok, parse_reply(<<"+OK\r\n">>)),
    ?assertEqual(queued, parse_reply(<<"+QUEUED\r\n">>)),
    ?assertEqual(pong, parse_reply(<<"+PONG\r\n">>)),
    ?assertEqual(<<"OTHER STATUS">>, parse_reply(<<"+OTHER STATUS\r\n">>)),

    ?assertEqual({error, <<"FORMAT">>}, parse_reply(<<"-FORMAT\r\n">>)),
    ?assertEqual({error, <<"UNKNOWN">>}, parse_reply(<<"-UNKNOWN\r\n">>)),

    ?assertEqual(0, parse_reply(<<":0\r\n">>)),
    ?assertEqual(1, parse_reply(<<":1\r\n">>)),
    ?assertEqual(231, parse_reply(<<":231\r\n">>)),
    ?assertEqual(987234, parse_reply(<<":987234\r\n">>)),
    ?assertEqual(-3, parse_reply(<<":-3\r\n">>)),

    ?assertEqual(null, parse_reply(<<"$-1\r\n">>)),
    ?assertEqual(<<>>, parse_reply(<<"$0\r\n">>)),
    ?assertEqual({bulk_more, 1}, parse_reply(<<"$1\r\n">>)),
    ?assertEqual({bulk_more, 10}, parse_reply(<<"$10\r\n">>)),
    ?assertEqual(<<"0123456789">>, parse_reply(<<"0123456789\r\n">>, {bulk_more, 10})),
    ?assertError({badmatch, _}, parse_reply(<<"0123456789">>, {bulk_more, 10})),

    MB1 = {mbulk_more, 2, [], next},
    MB21 = {mbulk_more, 2, [], {bulk_more, 5}},
    MB22 = {mbulk_more, 1, [<<"hello">>], next},
    MB31 = {mbulk_more, 1, [<<"hello">>], {bulk_more, 3}},
    MB32 = [<<"hello">>, <<"bob">>],
    ?assertEqual(null, parse_reply(<<"*-1\r\n">>)),
    ?assertEqual(null, parse_reply(<<"*0\r\n">>)),
    ?assertEqual(MB1, parse_reply(<<"*2\r\n">>)),
    ?assertEqual(MB21, parse_reply(<<"$5\r\n">>, MB1)),
    ?assertEqual(MB22, parse_reply(<<"hello\r\n">>, MB21)),
    ?assertEqual(MB31, parse_reply(<<"$3\r\n">>, MB22)),
    ?assertEqual(MB32, parse_reply(<<"bob\r\n">>,MB31)), 

    ?assertEqual({mbulk_more, 1, [], next}, parse_reply(<<"*1\r\n">>)),
    ?assertEqual([null], parse_reply(<<"$-1\r\n">>, {mbulk_more, 1, [], next})),

    ok.

-endif.
