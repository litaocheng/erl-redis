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

-export([mbulk/1, mbulk/2, mbulk/3, mbulk/4, mbulk_list/1]).

-export([parse_reply/1]).
-export([tokens/2]).

-compile([opt_bin_info]).
-compile({inline, 
        [mbulk/1, mbulk/2, mbulk/3, mbulk/4, mbulk_list/1, parse_reply/1]}).  

%% @doc generate the mbulk command
-spec mbulk(iodata()) -> iolist().
mbulk(Type) ->
    [<<"*1">>, ?CRLF, mbulk0(Type)].

-spec mbulk(iodata(), iodata()) ->
    iolist().
mbulk(Type, Arg) ->
    [<<"*2">>, ?CRLF, mbulk0(Type), mbulk0(Arg)].

-spec mbulk(iodata(), iodata(), iodata()) -> 
    iolist().
mbulk(Type, Arg1, Arg2) ->
    [<<"*3">>, ?CRLF, mbulk0(Type), mbulk0(Arg1), mbulk0(Arg2)].

-spec mbulk(iodata(), iodata(), iodata(), iodata()) -> 
    iolist().
mbulk(Type, Arg1, Arg2, Arg3) ->
    [<<"*4">>, ?CRLF, mbulk0(Type), mbulk0(Arg1), mbulk0(Arg2), mbulk0(Arg3)].

-spec mbulk_list(L :: [iodata()]) ->
    iolist().
mbulk_list(L) ->
    N = length(L),
    Lines = [mbulk0(E) || E <- L],
    [<<"*">>, ?N2S(N), ?CRLF, Lines].

%% @doc parse the reply
-spec parse_reply(Bin :: binary()) ->
    any().
parse_reply(<<"+", Rest/binary>>) ->
    parse_status_reply(Rest);
parse_reply(<<"-", Rest/binary>>) ->
    parse_error_reply(Rest);
parse_reply(<<":", Rest/binary>>) ->
    b2n(Rest);

parse_reply(<<"$-1\r\n">>) ->
    null;
parse_reply(<<"$0\r\n">>) ->
    {bulk_more, 0}; 
parse_reply(<<"$", Rest/binary>>) ->
    N = b2n(Rest),
    {bulk_more, N};
parse_reply(<<"*-1\r\n">>) ->
    null;
parse_reply(<<"*0\r\n">>) ->
    null;
parse_reply(<<"*", Rest/binary>>) ->
    N = b2n(Rest),
    {mbulk_more, N}.

%% @doc return a list of tokens in binary, separated by the character Sep
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
mbulk0(B) when is_binary(B) ->
    N = byte_size(B),
    ["$", ?N2S(N), ?CRLF, B, ?CRLF];
mbulk0(L) when is_list(L) ->
    N = iolist_size(L),
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
    ?assertEqual({bulk_more, 0}, parse_reply(<<"$0\r\n">>)),
    ?assertEqual({bulk_more, 1}, parse_reply(<<"$1\r\n">>)),
    ?assertEqual({bulk_more, 10}, parse_reply(<<"$10\r\n">>)),

    ?assertEqual(null, parse_reply(<<"*-1\r\n">>)),
    ?assertEqual(null, parse_reply(<<"*0\r\n">>)),
    ?assertEqual({mbulk_more, 2}, parse_reply(<<"*2\r\n">>)),

    ?assertEqual({mbulk_more, 1}, parse_reply(<<"*1\r\n">>)),

    ok.

-endif.
