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
-include("redis.hrl").

-export([line/1, line/2, line/3, line/4, line_list/1,
         bulk/3, bulk/4, mbulk/1]).
-export([parse_reply/2]).
-export([tokens/2]).

-compile([opt_bin_info]).
-compile({inline, [line/1, line/2, line/3, line/4, line_list/1,
         bulk/3, bulk/4, mbulk/1]}).  

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
-spec parse_reply(Bin :: binary(), Sock :: port()) ->
    any().
parse_reply(<<"+", Rest/binary>>, Sock) ->
    parse_status_reply(Rest, Sock);
parse_reply(<<"-", Rest/binary>>, Sock) ->
    parse_error_reply(Rest, Sock);
parse_reply(<<":", Rest/binary>>, Sock) ->
    parse_intger_reply(Rest, Sock);
parse_reply(<<"$", Rest/binary>>, Sock) ->
    parse_bulk_reply(Rest, Sock);
parse_reply(<<"*", Rest/binary>>, Sock) ->
    parse_mbulk_reply(Rest, Sock).

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
parse_status_reply(<<"OK\r\n">>, _Sock) ->
    ok;
parse_status_reply(<<"QUEUED\r\n">>, _Sock) ->
    queued;
parse_status_reply(<<"PONG\r\n">>, _Sock) ->
    pong;
parse_status_reply(<<"none\r\n">>, _Sock) ->
    none;
parse_status_reply(<<"string\r\n">>, _Sock) ->
    string;
parse_status_reply(<<"list\r\n">>, _Sock) ->
    list;
parse_status_reply(<<"set\r\n">>, _Sock) ->
    set;
parse_status_reply(Status, _Sock) ->
    Len = byte_size(Status) - 2,
    <<Val:Len/bytes, "\r\n">> = Status,
    Val.

%% parse error reply
parse_error_reply(Bin, _Sock) when is_binary(Bin) ->
    L = byte_size(Bin) - 2,
    <<Msg:L/bytes, "\r\n">> = Bin,
    {error, Msg}.

%% parse integer repley
parse_intger_reply(<<"0\r\n">>, _Sock) ->
    0;
parse_intger_reply(<<"1\r\n">>, _Sock) ->
    1;
parse_intger_reply(Bin, _Sock) ->
    b2n(Bin).
    
%% parse bulk reply
parse_bulk_reply(<<"-1\r\n">>, _Sock) ->
    null;
parse_bulk_reply(Bin, Sock) ->
    N = b2n(Bin),
    ok = inet:setopts(Sock, [{packet, raw}]),
    <<Val:N/bytes, _/binary>> = recv_n(Sock, N + 2),
    ok = inet:setopts(Sock, [{packet, line}]),
    Val.
     
%% parse multi bulk reply
parse_mbulk_reply(<<"-1\r\n">>, _Sock) ->
    null;
parse_mbulk_reply(Bin, Sock) ->
    N = b2n(Bin),
    lists:reverse(parse_mbulk_reply1(N, Sock, [])).

parse_mbulk_reply1(0, _Sock, Acc) ->
    Acc;
parse_mbulk_reply1(N, Sock, Acc) ->
    <<$$, Bin/bytes>> = recv_line(Sock),
    Bulk = parse_bulk_reply(Bin, Sock),
    parse_mbulk_reply1(N - 1, Sock, [Bulk | Acc]).
    
%% recv n bytes
recv_n(Sock, Len) ->
    case gen_tcp:recv(Sock, Len, ?RECV_TIMEOUT) of
        {ok, Bin} ->
            ?DEBUG2("recv_n :~p", [Bin]),
            Bin;
        {error, Reason} ->
            ?ERROR2("recv n bytes error:~p", [Reason]),
            throw({tcp_error, Reason})
    end.

%% recv line bytes
recv_line(Sock) ->
    case gen_tcp:recv(Sock, 0, ?RECV_TIMEOUT) of
        {ok, Bin} ->
            ?DEBUG2("recv_line :~p", [Bin]),
            Bin;
        {error, Reason} ->
            ?ERROR2("recv line error:~p", [Reason]),
            throw({tcp_error, Reason})
    end.

%% binary to integer
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
    parse_reply(Bin, {}).

parse_test_() ->
    [
        ?_assertEqual(ok, parse_reply(<<"+OK\r\n">>)),
        ?_assertEqual(queued, parse_reply(<<"+QUEUED\r\n">>)),
        ?_assertEqual(pong, parse_reply(<<"+PONG\r\n">>)),

        ?_assertEqual({error, <<"FORMAT">>}, parse_reply(<<"-FORMAT\r\n">>)),
        ?_assertEqual({error, <<"UNKNOWN">>}, parse_reply(<<"-UNKNOWN\r\n">>)),

        ?_assertEqual(0, parse_reply(<<":0\r\n">>)),
        ?_assertEqual(1, parse_reply(<<":1\r\n">>)),
        ?_assertEqual(231, parse_reply(<<":231\r\n">>)),
        ?_assertEqual(987234, parse_reply(<<":987234\r\n">>)),

        ?_assertEqual(null, parse_reply(<<"$-1\r\n">>)),
        ?_assertEqual(null, parse_reply(<<"*-1\r\n">>)),

        ?_assert(true)
    ].

-endif.
