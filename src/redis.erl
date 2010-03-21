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

%% connection pool size
-define(DEF_POOL, 2).

%% @doc show stats info in the stdout
-spec i() -> 'ok'.
i() ->
    ok.

%% @doc return the redis version info
%% first element is the redis client version,
%% second element is the lowest redis server version supported
-spec vsn() -> {string(), string()}.
vsn() ->
    {get_app_vsn(), "1.2.5"}.

%% @doc return the server info
-spec servers() -> [single_server()].
servers() ->
    redis_servers:server_info().

%% @doc return the server config type
-spec server_type() -> server_type().
server_type() ->
    redis_servers:server_type().

%% @doc set single redis server
-spec single_server(Host :: inet_host(), Port :: inet_port()) ->
    'ok' | {'error', any()}.
single_server(Host, Port) ->
    single_server(Host, Port, ?DEF_POOL).

%% @doc set single redis server, with the connection pool option
-spec single_server(Host :: inet_host(), Port :: inet_port(), Pool :: pos_integer()) ->
    'ok' | {'error', any()}.
single_server(Host, Port, Pool) ->
    case get_mode_env() of
        undefined ->
            Server = {Host, Port, Pool},
            Mode = {single, Server},
            % setup the connections in new process
            ok = redis_conn_sup:setup_connections(Mode),
            ok = set_mode_env(Mode);
        _ ->
            {error, already_set}
    end.

%% @doc set the multi servers 
-spec multi_servers(Servers :: [single_server()]) ->
    'ok' | {'error', any()}.
multi_servers(Servers) ->
    Dist = redis_dist:new(Servers),
    case get_mode_env() of
        undefined ->
            Mode = {dist, Dist},
            % setup the connections in new process
            ok = redis_conn_sup:setup_connections(Mode),
            ok = set_mode_env(Mode);
        _ ->
            {error, already_set}
    end.

%% @doc get server mode from application env
-spec get_mode_env() -> 'undefined' | {'ok', mode_info()}.
get_mode_env() ->
    application:get_env(redis, server_mode_info).

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
%% generic commands
%%

%% @doc test if the specified key exists
%% O(1)
-spec exists(Key :: key()) -> 
    boolean().
exists(Key) ->
    int_bool(call_key(<<"EXISTS">>, Key)).

%% @doc remove the specified key, return ture if deleted, 
%% otherwise return false
%% O(1)
-spec delete(Keys :: [key()]) -> 
    boolean().
delete(Key) ->
    int_bool(call_key(<<"DEL">>, Key)).

%% @doc remove the specified keys, return the number of
%% keys removed.
%% O(1)
-spec multi_delete(Keys :: [key()]) -> 
    'ok' | non_neg_integer().
multi_delete(Keys) ->
    lists:sum(call_keys(<<"DEL">>, Keys)).

%% @doc return the type of the value stored at key
%% O(1)
-spec type(Key :: key()) -> 
    value_type().
type(Key) ->
    call_key(<<"TYPE">>, Key).

%% @doc return the keys list matching a given pattern,
%% if all the servers reply the successed, return  {ok, [key()]},
%% otherwise return {error, [key()], [pid()]}, the third element in
%% return tuple is the bad clients.
%% O(n)
-spec keys(Pattern :: pattern()) -> 
    {[key()], [server()]}.
keys(Pattern) ->
    {Replies, BadServers} = call_all(cmd_line(<<"KEYS">>, Pattern)),
    Keys = lists:append([redis_proto:tokens(R, ?SEP) || {_Server, R} <- Replies]),
    {Keys, BadServers}.

%% @doc return a randomly selected key from the currently selected DB
%% O(1)
-spec random_key() -> 
    key() | null().
random_key() ->
    F = 
    fun
        (null) ->
            false;
        (_) ->
            true
    end,
    catch call_any(cmd_line(<<"RANDOMKEY">>), F).

%% @doc atmoically renames key OldKey to  NewKey
%% O(1)
%% ???
-spec rename(OldKey :: key(), NewKey :: key()) -> 
    'ok'.
rename(OldKey, NewKey) ->
    call(cmd_line(<<"RENAME">>, OldKey, NewKey)).

%% ???
-spec rename_not_exists(OldKey :: key(), NewKey :: key()) -> 
    'ok' | 'fail' | error_reply().
rename_not_exists(OldKey, NewKey) ->
    int_return(
        call(cmd_line(<<"RENAMENX">>, OldKey, NewKey))).
%% @doc return the nubmer of keys in all the currently selected database
%% O(1)
-spec dbsize() -> 
    {integer() , [server()]}.
dbsize() ->
    {Replies, BadServers} = call_all(cmd_line(<<"DBSIZE">>)),
    
    Size = 
    lists:foldl(
        fun({_Server, R}, Acc) ->
            R + Acc
        end,
    0, Replies),
    {Size, BadServers}.

%% @doc set a timeout on the specified key, after the Time the Key will
%% automatically deleted by server.
%% O(1)
-spec expire(Key :: key(), Time :: second()) -> 
    boolean().
expire(Key, Time) ->
    int_bool(
        call_key(<<"EXPIRE">>, Key, ?N2S(Time))).

%% @doc set a unix timestamp on the specified key, the Key will
%% automatically deleted by server at the TimeStamp in the future.
%% O(1)
-spec expire_at(Key :: key(), TimeStamp :: timestamp()) -> 
    boolean().
expire_at(Key, TimeStamp) ->
    Str = ?N2S(TimeStamp),
    int_bool(
        call_key(<<"EXPIREAT">>, Key, Str)).

%% @doc return the remaining time to live in seconds of a key that
%% has an EXPIRE set
%% O(1)
-spec ttl(Key :: key()) -> non_neg_integer().
ttl(Key) ->
    call_key(<<"TTL">>, Key).

%% @doc select the DB with have the specified zero-based numeric index
-spec select(Index :: index()) ->
    'ok' | {'error', [server()]}.
select(Index) ->
    {_Replies, BadServers} = call_all(cmd_line(<<"SELECT">>, ?N2S(Index))),
    case BadServers of
        [] ->
            ok;
        [_|_] ->
            {error, BadServers}
    end.

%% @doc move the specified key  from the currently selected DB to the specified
%% destination DB
-spec move(Key :: key(), DBIndex :: index()) ->
    boolean() | error_reply().
move(Key, DBIndex) ->
    int_may_bool(
        call_key(<<"MOVE">>, Key, ?N2S(DBIndex))).

%% @doc delete all the keys of the currently selected DB
-spec flush_db() -> 'ok' | {'error', [server()]}.
flush_db() ->
    {_Replies, BadServers} = call_all(cmd_line(<<"FLUSHDB">>)),
    case BadServers of
        [] ->
            ok;
        [_|_] ->
            {error, BadServers}
    end.
            
-spec flush_all() -> 'ok' | {'error', [server()]}.
flush_all() ->
    {_Replies, BadServers} = call_all(cmd_line(<<"FLUSHALL">>)),
    case BadServers of
        [] ->
            ok;
        [_|_] ->
            {error, BadServers}
    end.

%%
%% string commands 
%%

%% @doc set the string as value of the key
%% O(1)
-spec set(Key :: key(), Val :: string_value()) ->
    'ok'.
set(Key, Val) ->
    L1 = cmd_line(<<"SET">>, Key, ?N2S(iolist_size(Val))),
    L2 = cmd_line(Val),
    do_call_key(Key, [L1, L2]).
    
%% @doc get the value of specified key
%% O(1)
-spec get(Key :: key()) ->
    null() | binary().
get(Key) ->
    call_key(<<"GET">>, Key).

%% @doc atomatic set the value and return the old value
%% O(1)
-spec getset(Key :: key(), Val :: string_value()) ->
    null() | binary() | error_reply(). 
getset(Key, Val) ->
    L1 = cmd_line(<<"GETSET">>, Key, ?N2S(iolist_size(Val))),
    L2 = cmd_line(Val),
    do_call_key(Key, [L1, L2]).

-spec multi_get(Keys :: [key()]) ->
    [string() | null()].
multi_get(Keys) ->
    call(cmd_line([<<"MGET">> | Keys])).

-spec set_not_exists(Key :: key(), Val :: string()) -> 
    'ok' | 'fail' | error_reply().
set_not_exists(Key, Val) ->
    int_return(
        call(cmd_line(<<"SETNX">>, Key, Val))).

-spec multi_set(Keys :: [key()]) ->
    'ok'.
multi_set(Keys) ->
    status_return(
        call(cmd_line([<<"MSET">> | Keys]))).

-spec multi_set_not_exists(Keys :: [key()]) ->
    'ok' | 'fail'.
multi_set_not_exists(Keys) ->
    int_bool(
        call(cmd_line([<<"MSETNX">> | Keys]))).

-spec incr(Key :: key()) -> 
    integer().
incr(Key) ->
    call(cmd_line(<<"INCR">>, Key)).

-spec incr(Key :: key(), N :: integer()) -> 
    integer().
incr(Key, N) ->
    call(cmd_line(<<"INCRBY">>, Key, ?N2S(N))).

-spec decr(Key :: key()) -> 
    integer().
decr(Key) ->
    call(cmd_line(<<"DECR">>, Key)).

-spec decr(Key :: key(), N :: integer()) -> 
    integer().
decr(Key, N) ->
    call(cmd_line(<<"DECRBY">>, Key, ?N2S(N))).

%%------------------------------------------------------------------------------
%%
%% internal API
%%
%%------------------------------------------------------------------------------

%% 
call(_Cmd) ->
    ok.

%% do the call to all the servers
call_all(Cmd) ->
    {ok, Clients} = redis_servers:get_all_clients(),
    ?DEBUG2("call all send cmd ~p by clients:~p", [Cmd, Clients]),
    {Replies, BadClients} = redis_client:multi_send(Clients, Cmd),
    {[{redis_client:get_server(Pid), R} || {Pid, R} <- Replies],
     [redis_client:get_server(Pid) || Pid <- BadClients]}.


%% do the call to any one server
call_any(Cmd, F) ->
    {ok, Clients} = redis_servers:get_all_clients(),
    ?DEBUG2("call any send cmd ~p by clients:~p", [Cmd, Clients]),
    [V | _] =
    [begin
        Ret = redis_client:send(Client, Cmd),
        case F(Ret) of
            true ->
                throw(Ret);
            false ->
                Ret
        end
    end || Client <- Clients],
    V.

%% do the call with key
call_key(Type, Key) ->
    do_call_key(Key, cmd_line(Type, Key)).
call_key(Type, Key, Arg1) ->
    do_call_key(Key, cmd_line(Type, Key, Arg1)).
call_key(Type, Key, Arg1, Arg2) ->
    do_call_key(Key, cmd_line(Type, Key, Arg1, Arg2)).
do_call_key(Key, Cmd) ->
    {ok, Client} = redis_servers:get_client(Key),
    redis_client:send(Client, Cmd).

%% do the call with keys
call_keys(Type, Keys) ->
    [call_key(Type, Key) || Key <- Keys].

%% convert status code to return
status_return(<<"OK">>) -> ok;
status_return(S) -> S.

%% convert integer to return
int_return(0) -> false;
int_return(1) -> true.

int_bool(0) -> false;
int_bool(1) -> true.

int_may_bool(0) -> false;
int_may_bool(1) -> true;
int_may_bool(V) -> V.

%% generate the single line
cmd_line(Type) ->
    [Type, ?CRLF_BIN].

cmd_line(Type, Arg) ->
    [Type, ?SEP_BIN, Arg, ?CRLF_BIN].

cmd_line(Type, Arg1, Arg2) ->
    [Type, ?SEP_BIN, Arg1, ?SEP_BIN, Arg2, ?CRLF_BIN].

cmd_line(Type, Arg1, Arg2, Arg3) ->
    [Type, ?SEP_BIN, Arg1, ?SEP_BIN, Arg2, ?SEP_BIN, Arg3, ?CRLF_BIN].

cmd_line_list(Parts) ->
    [?SEP_BIN | Line] = 
    lists:foldr(
        fun(P, Acc) ->
            [?SEP_BIN, P | Acc]
        end,
    [?CRLF_BIN],
    Parts),
    Line.

%% set server mode to application env
set_mode_env(Mode) ->
    application:set_env(redis, server_mode_info, Mode).

%% get the application vsn
%% return 'undefined' | string().
get_app_vsn() ->
    {ok, App} = application:get_application(),
    {ok, Vsn} = application:get_key(App, vsn),
    Vsn.

-ifdef(TEST).

l2b(Line) ->
    iolist_to_binary(Line).

cmd_line_test_() ->
    [
        ?_assertEqual(<<"EXISTS key1\r\n">>, l2b(cmd_line(<<"EXISTS">>, "key1"))),
        ?_assertEqual(<<"EXISTS key2\r\n">>, l2b(cmd_line("EXISTS", "key2"))),
        ?_assertEqual(<<"type key1 key2\r\n">>, l2b(cmd_line("type", "key1", <<"key2">>))),
        ?_assertEqual(<<"type key1 key2\r\n">>, l2b(cmd_line(["type", <<"key1">>, "key2"]))),

        ?_assert(true)
    ].

-endif.
