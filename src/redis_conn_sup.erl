%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litao cheng <litaocheng@gmail.com>
%%% @doc redis connection supervisor
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis_conn_sup).
-author('litaocheng@gmail.com').
-vsn('0.1').
-include("redis.hrl").

-behaviour(supervisor).

-export([start_link/0]).
-export([setup_connections/1]).
-export([init/1]).


%% @doc the application start callback
-spec start_link() -> 
    {'ok', pid()} | 'ignore' | {'error', any()}.
start_link() ->
    ?INFO2("start the supervisor", []),
    Ret = {ok, _} = supervisor:start_link({local, ?CONN_SUP}, ?MODULE, []),
    % decide if the supervisor is restarted by check set redis mode env
    case redis:get_mode_env() of
        undefined ->
            Ret;
        {ok, Mode} ->
            ?DEBUG2("find mode env so setup the connections", []),
            case setup_connections(Mode) of
                ok ->
                    Ret;
                Error ->
                    Error
            end
    end.

%% @doc setup connections
-spec setup_connections(Mode :: mode_info()) ->
    'ok' | {error, any()}.
setup_connections(Mode) ->
    ?DEBUG2("setup the connections to ~p", [Mode]),
    ok = redis_servers:set_mode(Mode),

    Servers = 
    case Mode of
        {single, Server} ->
            [Server];
        {dist, Dist} ->
            redis_dist:to_list(Dist)
    end,

    Errors =
    lists:foldl(
        fun({Host, Port, Pool}, Acc) ->
            case catch setup_pool(Host, Port, Pool) of
                ok ->
                    Acc;
                {error, Reason} ->
                    [{{Host, Port}, Reason} | Acc]
            end
        end,
    [], Servers),

    case Errors of
        [] ->
            ok;
        [_|_] ->
            {error, Errors}
    end.

%% @doc the connection supervisor callback
init([]) -> 
    ?DEBUG2("init supervisor", []),
    Stragegy = {simple_one_for_one, 10, 10},
    Client = {undefined, {redis_client, start_link, []},
                permanent, 1000, worker, [redis_client]},

    {ok, {Stragegy, [Client]}}.

%%
%% internal API
%%

%% start an new redis_client process connect the specify redis server
connect(Host, Port, Index, Timeout) ->
    supervisor:start_child(?CONN_SUP, [{Host, Port}, Index, Timeout]).

%% setup connecton pool with one server
setup_pool(Host, Port, Pool) ->
   [begin 
        case connect(Host, Port, N, ?CONN_TIMEOUT) of
            {error, Reason} ->
                ?ERROR2("make the ~p connection to redis server ~p:~p error:~p", 
                    [N, Host, Port, Reason]),
                throw({error, Reason});
            {ok, _Client} ->
                ok
        end
    end || N <- lists:seq(1, Pool)],
    ok.

