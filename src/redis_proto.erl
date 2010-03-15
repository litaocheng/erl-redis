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

-compile([opt_bin_info]).
-export([parse_reply/2]).
-export([tokens/2]).

%% @doc parse the reply
-spec parse_reply(Bin :: binary(), Conn :: connection()) ->
    any().
parse_reply(<<"+", Rest/binary>>, Conn) ->
    parse_status_reply(Rest, Conn);
parse_reply(<<"-", Rest/binary>>, Conn) ->
    parse_error_reply(Rest, Conn);
parse_reply(<<":", Rest/binary>>, Conn) ->
    parse_intger_reply(Rest, Conn);
parse_reply(<<"$", Rest/binary>>, Conn) ->
    parse_bulk_reply(Rest, Conn);
parse_reply(<<"*", Rest/binary>>, Conn) ->
    parse_mbulk_reply(Rest, Conn).

%% @doc return a list of tokens in string, separated by the characters
%%  in Separatorlist
-spec tokens(S :: binary(), Sep :: char()) -> [binary()].
tokens(S, Sep) ->
    tokens1(S, Sep, []).

%%
%%------------------------------------------------------------------------------
%%
%% internal API
%%
%%------------------------------------------------------------------------------

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
parse_status_reply(<<"OK">>, _Conn) ->
    ok;
parse_status_reply(<<"QUEUED">>, _Conn) ->
    queued;
parse_status_reply(<<"PONG">>, _Conn) ->
    pong.

%% parse error reply
parse_error_reply(Msg, _Conn) when is_binary(Msg) ->
    {error, Msg}.

%% parse integer repley
parse_intger_reply(<<"0">>, _Conn) ->
    0;
parse_intger_reply(<<"1">>, _Conn) ->
    1;
parse_intger_reply(Bin, _Conn) ->
    b2n(Bin).
    
%% parse bulk reply
parse_bulk_reply(<<"-1">>, _Conn) ->
    none;
parse_bulk_reply(Bin, {_, Sock}) ->
    N = b2n(Bin),
    ok = inet:setopts(Sock, [{packet, raw}]),
    <<Val:N, _/binary>> = recv_n(Sock, N + 2),
    ok = inet:setopts(Sock, [{packet, line}]),
    Val.
     
%% parse multi bulk reply
parse_mbulk_reply(<<"-1">>, _Conn) ->
    none;
parse_mbulk_reply(Bin, Conn) ->
    N = b2n(Bin),
    parse_mbulk_reply1(N, Conn, []).

parse_mbulk_reply1(0, _Conn, Acc) ->
    Acc;
parse_mbulk_reply1(N, {_, Sock} = Conn, Acc) ->
    <<$$, Bin>> = recv_line(Sock),
    Bulk = parse_bulk_reply(Bin, Conn),
    parse_mbulk_reply1(N - 1, Conn, [Bulk | Acc]).
    
%% recv n bytes
recv_n(Sock, Len) ->
    case gen_tcp:recv(Sock, Len, ?RECV_TIMEOUT) of
        {ok, Bin} ->
            Bin;
        {error, Reason} ->
            ?ERROR2("recv n bytes error:~p", [Reason]),
            throw({error, Reason})
    end.

%% recv line bytes
recv_line(Sock) ->
    case gen_tcp:recv(Sock, 0, ?RECV_TIMEOUT) of
        {ok, Bin} ->
            Bin;
        {error, Reason} ->
            ?ERROR2("recv line error:~p", [Reason]),
            throw({error, Reason})
    end.

%% binary to integer
b2n(Bin) ->
    b2n(Bin, 0).

b2n(<<C, Rest/binary>>, N) when C >= $0, C =< $9 ->
    b2n(Rest, N * 10 + (C - $0));
b2n(<<>>, N) ->
    N.

-ifdef(TEST).

b2n_test_() ->
    [
        ?_assertEqual(233, b2n(<<"233">>)),     
        ?_assertEqual(0, b2n(<<"0">>)),     
        ?_assertEqual(123, b2n(<<"123">>)),     
        ?_assertEqual(12432, b2n(<<"12432">>))     
    ].

parse_reply(Bin) ->
    parse_reply(Bin, {}).

parse_test_() ->
    [
        ?_assertEqual(ok, parse_reply(<<"+OK">>)),
        ?_assertEqual(queued, parse_reply(<<"+QUEUED">>)),
        ?_assertEqual(pong, parse_reply(<<"+PONG">>)),

        ?_assertEqual({error, <<"FORMAT">>}, parse_reply(<<"-FORMAT">>)),
        ?_assertEqual({error, <<"UNKNOWN">>}, parse_reply(<<"-UNKNOWN">>)),

        ?_assertEqual(0, parse_reply(<<":0">>)),
        ?_assertEqual(1, parse_reply(<<":1">>)),
        ?_assertEqual(231, parse_reply(<<":231">>)),
        ?_assertEqual(987234, parse_reply(<<":987234">>)),

        ?_assertEqual(none, parse_reply(<<"$-1">>)),
        ?_assertEqual(none, parse_reply(<<"*-1">>)),

        ?_assert(true)
    ].

-endif.
