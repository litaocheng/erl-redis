%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litao cheng <litaocheng@gmail.com>
%%% @doc redis app and supervisor callback
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis_app).
-author('litaocheng@gmail.com').
-vsn('0.1').
-include("redis_internal.hrl").

-behaviour(application).
-behaviour(supervisor).

-export([start/0, stop/0]).
-export([single_server/3, single_server/4]).
-export([dist_server/1, dist_server/2]).
-export([group_server/4]).
-export([client/1, client/2]).

-export([start/2, stop/1]).
-export([init/1]).

-define(GROUP_DEFAULT, '$default').

-spec start() -> 'ok' | {'error', any()}.
start() ->
    ?DEBUG2("start the ~p application", [?MODULE]),
    application:start(redis).

-spec stop() -> 'ok'.
stop() ->
    ?DEBUG2("stop the ~p application", [?MODULE]),
    application:stop(redis).

-spec single_server(Host :: inet_host(), Port :: inet_port(), Pool :: pos_integer()) ->
    'ok' | {'error', 'already_started'}.
single_server(Host, Port, Pool) when Pool >= ?CONN_POOL_MIN,
                                    Pool =< ?CONN_POOL_MAX ->
    single_server(Host, Port, Pool, "").

-spec single_server(Host :: inet_host(), Port :: inet_port(), 
        Pool :: pos_integer(), Passwd :: passwd()) ->
    'ok' | {'error', 'already_started'}.
single_server(Host, Port, Pool, Passwd) ->
    start_manager(?GROUP_DEFAULT, {single, {Host, Port, Pool}}, Passwd).

-spec dist_server(Servers :: [single_server()]) ->
    'ok' | {'error', 'already_started'}.
dist_server(Servers) ->
    dist_server(Servers, "").

-spec dist_server(Servers :: [single_server()], Passwd :: passwd()) ->
    'ok' | {'error', 'already_started'}.
dist_server(Servers, Passwd) ->
    start_manager(?GROUP_DEFAULT, {dist, redis_dist:new(Servers)}, Passwd).

group_server(Group, Type, Server, Pwd) ->
    Mode = 
    case Type of
        single ->
            {Type, Server};
        dist ->
            {Type, redis_dist:new(Server)}
    end,
    start_manager(Group, Mode, Pwd).

-spec client(server_type()) -> tuple().
client(Type) ->
    redis:new(manager_name(?GROUP_DEFAULT, Type),
        ?GROUP_DEFAULT,
        Type).

-spec client(group(), server_type()) -> tuple().
client(Group, Type) ->
    redis:new(manager_name(Group, Type),
        Group, 
        Type).

%% start the redis server manager
start_manager(Group, {Type, _} = Mode, Passwd) ->
    Name = manager_name(Group, Type),
    Man = {Name, {redis_manager, start_link, [Name, Mode, Passwd]},
                permanent, 1000, worker, [redis_manager]},
    case supervisor:start_child(?REDIS_SUP, Man) of
        {ok, _Pid} ->
            ok;
        {ok, _Pid, _Info} ->
            ok;
        Other ->
            Other
    end.

%% @doc the application start callback
-spec start(Type :: any(), Args :: any()) -> any().
start(_Type, _Args) ->
    ?INFO2("start the supervisor", []),
    supervisor:start_link({local, ?REDIS_SUP}, ?MODULE, []).

%% @doc the application  stop callback
stop(_State) ->
    ok.

%% @doc the main supervisor and connection supervisor callbacks
init([]) -> 
    ?DEBUG2("init supervisor", []),
    Stragegy = {one_for_all, 10, 10},

    ConnSup = {?CONN_SUP, {redis_conn_sup, start_link, []},
                permanent, infinity, supervisor, [redis_conn_sup]},

    {ok, {Stragegy, [ConnSup]}}.

%%
%% internal API
%%

manager_name(Group, Type) when is_atom(Group) ->
    list_to_atom(lists:concat([?MANAGER_BASE, '_', Group, '_', Type])).
