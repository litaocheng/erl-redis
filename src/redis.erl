%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng@gmail.com
%%% @doc the interface for redis
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis).
-author('ltiaocheng@gmail.com').
-vsn('0.1').
-include("redis.hrl").

-export([i/0]).
-compile([export_all]).


%% @doc show stats info in the stdout
-spec i() -> 'ok'.
i() ->
    ok.

%% @doc set single redis server
-spec set_server(Host :: inet_host(), Port :: inet_port()) ->
    'ok' | {'error', any()}.
set_server(Host, Port) ->
    redis_client:set_server(single, {Host, Port}).

%% @doc set the distributed servers info 
-spec set_dist(Dist :: dist_info()) ->
    'ok' | {'error', any()}.
set_dist(Dist) ->
    redis_client:set_server(dist, Dist).

%% @doc set the max connections for per server
max_conn_per_server(_N) ->
    false.

%%
%% Connection handling
%%
-spec auth(Passwd :: passwd()) ->
    'ok'.
auth(Passwd) ->
    redis_servers:set_passwd(Passwd).

-spec auth(Server :: single_server(), Passwd :: passwd()) ->
    'ok'.
auth(Server, Passwd) ->
    redis_servers:set_passwd(Server, Passwd).

%%
%% commands operating on all the kind of values
%%
-spec exists(Key :: key()) -> 
    boolean().
exists(Key) ->
    call_key(<<"EXIST">>, Key).

-spec delete(Key :: key()) -> 
    'ok' | 'fail'.
delete(Key) ->
    int_return(
        call(single_line(<<"DEL">>, Key))).

-spec multi_delete(Keys :: [key()]) -> 
    'ok' | non_neg_integer().
multi_delete(Keys) ->
    Len = length(Keys),
    case call(single_line([<<"DEL">> | Keys])) of
        Len ->
            ok;
        N ->
            N
    end.

-spec type(Key :: key()) -> 
    value_type().
type(Key) ->
    call(single_line(<<"TYPE">>, Key)).

-spec keys(Pattern :: pattern()) -> 
    [key()].
keys(Pattern) ->
    call(single_line(<<"KEYS">>, Pattern)).

-spec random_key() -> 
    key() | nil().
random_key() ->
    call(<<"RANDOMKEY",?CRLF_BIN/binary>>).

-spec rename(OldKey :: key(), NewKey :: key()) -> 
    'ok'.
rename(OldKey, NewKey) ->
    call(single_line(<<"RENAME">>, OldKey, NewKey)).

-spec rename_not_exists(OldKey :: key(), NewKey :: key()) -> 
    'ok' | 'fail' | error_reply().
rename_not_exists(OldKey, NewKey) ->
    int_return(
        call(single_line(<<"RENAMENX">>, OldKey, NewKey))).

-spec dbsize() -> 
    integer().
dbsize() ->
    call(<<"DBSIZE", ?CRLF_BIN/binary>>).

-spec expire(Key :: key(), Time :: second()) -> 
    'ok' | error_reply().
expire(Key, Time) ->
    int_return(
        call(single_line(<<"EXPIRE">>, Key, ?N2S(Time)))).

-spec ttl(Key :: key()) -> 
    integer() | error_reply().
ttl(Key) ->
    call(single_line(<<"TTL">>, Key)).

-spec select(Index :: index()) ->
    'ok' | error_reply().
select(Index) ->
    status_return(
        call(single_line(<<"SELECT">>, ?N2S(Index)))).

-spec move(Key :: key(), DBIndex :: index()) ->
    'ok' | 'fail'.
move(Key, DBIndex) ->
    int_return(
        call(single_line(<<"MOVE">>, Key, ?N2S(DBIndex)))).

-spec flush_db() ->
    'ok'.
flush_db() ->
    status_return(call(single_line(<<"FLUSHDB", ?CRLF_BIN/binary>>))).

-spec flush_all() ->
    'ok'.
flush_all() ->
    status_return(call(single_line(<<"FLUSHALL", ?CRLF_BIN/binary>>))).

%%
%% commands operating on string values
%%
-spec set(Key :: key(), Val :: string()) ->
    'ok'.
set(Key, Val) ->
    status_return(
        call(single_line(<<"SET">>, Key, Val))).
    
-spec get(Key :: key()) ->
    nil() | string().
get(Key) ->
    call(single_line(<<"GET">>, Key)).

-spec getset(Key :: key(), Val :: string()) ->
    nil() | {'ok', string()} | error_reply(). 
getset(Key, Val) ->
    call(single_line(<<"GETSET">>, Key, Val)).

-spec multi_get(Keys :: [key()]) ->
    [string() | nil()].
multi_get(Keys) ->
    call(single_line([<<"MGET">> | Keys])).

-spec set_not_exists(Key :: key(), Val :: string()) -> 
    'ok' | 'fail' | error_reply().
set_not_exists(Key, Val) ->
    int_return(
        call(single_line(<<"SETNX">>, Key, Val))).

-spec multi_set(Keys :: [key()]) ->
    'ok'.
multi_set(Keys) ->
    status_return(
        call(single_line([<<"MSET">> | Keys]))).

-spec multi_set_not_exists(Keys :: [key()]) ->
    'ok' | 'fail'.
multi_set_not_exists(Keys) ->
    int_return(
        call(single_line([<<"MSETNX">> | Keys]))).

-spec incr(Key :: key()) -> 
    integer().
incr(Key) ->
    call(single_line(<<"INCR">>, Key)).

-spec incr(Key :: key(), N :: integer()) -> 
    integer().
incr(Key, N) ->
    call(single_line(<<"INCRBY">>, Key, ?N2S(N))).

-spec decr(Key :: key()) -> 
    integer().
decr(Key) ->
    call(single_line(<<"DECR">>, Key)).

-spec decr(Key :: key(), N :: integer()) -> 
    integer().
decr(Key, N) ->
    call(single_line(<<"DECRBY">>, Key, ?N2S(N))).

%%------------------------------------------------------------------------------
%%
%% internal API
%%
%%------------------------------------------------------------------------------

%% do the call
call(Cmd) ->
    redis_client:send(ok, Cmd).

%% do the call with key
call_key(Type, Key) ->
    {ok, Client} = redis_servers:get_client(Key),
    ?DEBUG2("send cmd [~p, ~p]  by client:~p", [Type, Key, Client]),
    redis_client:send(Client, single_line(Type, Key)).

%% convert status code to return
status_return(<<"OK">>) -> ok;
status_return(S) -> S.

%% convert integer to return
int_return(0) -> fail;
int_return(1) -> ok.

%% generate the single line
single_line(Type, Arg) ->
    [Type, ?SEP_BIN, Arg, ?CRLF_BIN].

single_line(Type, Arg1, Arg2) ->
    [Type, ?SEP_BIN, Arg1, ?SEP_BIN, Arg2, ?CRLF_BIN].

single_line(Parts) ->
    [?SEP_BIN | Line] = 
    lists:foldr(
        fun(P, Acc) ->
            [?SEP_BIN, P | Acc]
        end,
    [?CRLF_BIN],
    Parts),
    Line.

-ifdef(TEST).

l2b(Line) ->
    iolist_to_binary(Line).

single_line_test_() ->
    [
        ?_assertEqual(<<"EXISTS key1\r\n">>, l2b(single_line(<<"EXISTS">>, "key1"))),
        ?_assertEqual(<<"EXISTS key2\r\n">>, l2b(single_line("EXISTS", "key2"))),
        ?_assertEqual(<<"type key1 key2\r\n">>, l2b(single_line("type", "key1", <<"key2">>))),
        ?_assertEqual(<<"type key1 key2\r\n">>, l2b(single_line(["type", <<"key1">>, "key2"]))),

        ?_assert(true)
    ].

-endif.
