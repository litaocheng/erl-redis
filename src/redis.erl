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
    redis_manager:server_info().

%% @doc return the server config type
-spec server_type() -> server_type().
server_type() ->
    redis_manager:server_type().

%% @doc set single redis server
-spec single_server(Host :: inet_host(), Port :: inet_port()) ->
    'ok' | {'error', 'already_started'}.
single_server(Host, Port) ->
    single_server(Host, Port, ?DEF_POOL).

%% @doc set single redis server, with the connection pool option
-spec single_server(Host :: inet_host(), Port :: inet_port(), Pool :: pos_integer()) ->
    'ok' | {'error', 'already_started'}.
single_server(Host, Port, Pool) ->
    single_server(Host, Port, Pool, "").

%% @doc set single redis server, with the connection pool and passwd options
-spec single_server(Host :: inet_host(), Port :: inet_port(), 
        Pool :: pos_integer(), Passwd :: passwd()) ->
    'ok' | {'error', 'already_started'}.
single_server(Host, Port, Pool, Passwd) ->
    case redis_app:is_manager_started() of
        false ->
            redis_app:start_manager({single, {Host, Port, Pool}}, Passwd);
        true ->
            {error, already_started}
    end.

%% @doc set the multi servers 
-spec multi_servers(Servers :: [single_server()]) ->
    'ok' | {'error', 'already_started'}.
multi_servers(Servers) ->
    multi_servers(Servers, "").

%% @doc set the multiple servers, with passwd option
-spec multi_servers(Servers :: [single_server()], Passwd :: passwd()) ->
    'ok' | {'error', 'already_started'}.
multi_servers(Servers, Passwd) ->
    case redis_app:is_manager_started() of
        false ->
            redis_app:start_manager({dist, redis_dist:new(Servers)}, Passwd);
        true ->
            {error, already_started}
    end.

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
    do_set(<<"SET">>, Key, Val).
    
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

%% @doc get the values of all the specified keys
%% O(1)
-spec multi_get(Keys :: [key()]) ->
    [string() | null()].
multi_get(Keys) ->
    lists:append(call_keys(<<"MGET">>, Keys)).

%% @doc set value, if the key already exists no operation is performed
%% O(1)
-spec set_not_exists(Key :: key(), Val :: string()) -> 
    boolean().
set_not_exists(Key, Val) ->
    int_bool(
        do_set(<<"SETNX">>, Key, Val)).

%% @doc set the respective keys to respective values
%% O(1)
-spec multi_set(KeyVals :: [{key(), string_value()}]) ->
    'ok' | {'error', [key()]}.
multi_set(KeyVals) ->
    Type = <<"MSET">>,
    ClientKeys = redis_manager:partition_keys(KeyVals, fun({K, _V}) -> K end),

    Errors = 
    lists:foldl(
        fun({Client, KVs}, Acc) ->
            KVList = lists:append([[K, V] || {K, V} <- KVs]),
            L = [Type | KVList],
            case mbulk_cmd(Client, L) of
                ok ->
                    Acc;
                _ ->
                    {KL, _VL} = lists:unzip(KVs),
                    KL ++ Acc
            end
        end,
    [], ClientKeys),
    case Errors of
        [] ->
            ok;
        [_|_] ->
            {error, Errors}
    end.

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

%% set api
do_set(Type, Key, Val) ->
    L1 = cmd_line(Type, Key, ?N2S(iolist_size(Val))),
    L2 = cmd_line(Val),
    do_call_key(Key, [L1, L2]).

%% 
call(_Cmd) ->
    ok.

%% do the call to all the servers
call_all(Cmd) ->
    {ok, Clients} = redis_manager:get_all_clients(),
    ?DEBUG2("call all send cmd ~p by clients:~p", [Cmd, Clients]),
    {Replies, BadClients} = redis_client:multi_send(Clients, Cmd),
    {[{redis_client:get_server(Pid), R} || {Pid, R} <- Replies],
     [redis_client:get_server(Pid) || Pid <- BadClients]}.


%% do the call to any one server
call_any(Cmd, F) ->
    {ok, Clients} = redis_manager:get_all_clients(),
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
    {ok, Client} = redis_manager:get_client(Key),
    redis_client:send(Client, Cmd).

%% do the call with keys
call_keys(Type, Keys) ->
    [call_key(Type, Key) || Key <- Keys].

%% send the mbulk command
mbulk_cmd(Client, L) ->
    Count = length(L),
    Lines = [mbulk_line(E) || E <- L],
    redis_client:send(Client, ["*", ?N2S(Count), ?CRLF_BIN, Lines]).

%% generate the single line
cmd_line(Type) ->
    [Type, ?CRLF_BIN].

cmd_line(Type, Arg) ->
    [Type, ?SEP_BIN, Arg, ?CRLF_BIN].

cmd_line(Type, Arg1, Arg2) ->
    [Type, ?SEP_BIN, Arg1, ?SEP_BIN, Arg2, ?CRLF_BIN].

cmd_line(Type, Arg1, Arg2, Arg3) ->
    [Type, ?SEP_BIN, Arg1, ?SEP_BIN, Arg2, ?SEP_BIN, Arg3, ?CRLF_BIN].

%% concat the Parts into a single line
cmd_line_list(Parts) ->
    [?SEP_BIN | Line] = 
    lists:foldr(
        fun(P, Acc) ->
            [?SEP_BIN, P | Acc]
        end,
    [?CRLF_BIN],
    Parts),
    Line.

%% mbulk commands line
mbulk_line(D) when is_binary(D) ->
    N = byte_size(D),
    ["$", ?N2S(N), ?CRLF_BIN,  
     D, ?CRLF_BIN];
mbulk_line(D) when is_list(D) ->
    N = length(D),
    ["$", ?N2S(N), ?CRLF_BIN,
    D, ?CRLF_BIN].

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
        ?_assertEqual(<<"type key1 key2\r\n">>, l2b(cmd_line_list(["type", <<"key1">>, "key2"]))),

        ?_assert(true)
    ].

-endif.
