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
-include("redis.hrl").

-behaviour(application).
-behaviour(supervisor).

-export([start/0]).
-export([start/2, stop/1]).
-export([init/1]).

-define(MAIN_SUP, redis_sup).

%% @doc start the application from the erl shell
-spec start() -> 'ok' | {'error', any()}.
start() ->
    ?DEBUG2("start the ~p application", [?MODULE]),
    application:start(redis).

%% @doc the application start callback
-spec start(Type :: any(), Args :: any()) -> any().
start(_Type, _Args) ->
    ?INFO2("start the supervisor sup", []),
    supervisor:start_link({local, ?MAIN_SUP}, ?MODULE, []).

%% @doc the application  stop callback
stop(_State) ->
    ok.

%% @doc supervisor callback
init(_Args) -> 
    ?DEBUG2("init supervisor~n", []),
    Stragegy = {simple_one_for_one, 10, 10},
    Client = {undefined, {redis_client, start_link, []},
                permanent, 1000, worker, [redis_client]},

    {ok, {Stragegy, [Client]}}.

%%
%% internal API
%%

