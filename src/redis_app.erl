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

-export([start/0]).
-export([start_manager/2]).
-export([start/2, stop/1]).
-export([init/1]).

%% @doc start the application from the erl shell
-spec start() -> 'ok' | {'error', any()}.
start() ->
    ?DEBUG2("start the ~p application", [?MODULE]),
    application:start(redis).

%% @doc start the redis server manager
-spec start_manager(Mode :: mode_info(), Passwd :: passwd()) ->
    'ok' | {'error', any()}.
start_manager(Mode, Passwd) ->
    Man = {redis_manager, {redis_manager, start_link, [Mode, Passwd]},
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

