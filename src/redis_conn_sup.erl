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
-export([connect/3, connect/4]).
-export([init/1]).


%% @doc the application start callback
-spec start_link() -> 
    {'ok', pid()} | 'ignore' | {'error', any()}.
start_link() ->
    ?INFO2("start the supervisor", []),
    supervisor:start_link({local, ?CONN_SUP}, ?MODULE, []).

%% @doc start an new redis_client process connect the specify redis server
-spec connect(Host :: inet_host(), Port :: inet_port(), Index :: pos_integer()) ->
    {'ok', pid()} | 'ignore' | {'error', any()}.
connect(Host, Port, Index) ->
    connect(Host, Port, Index, ?CONN_TIMEOUT).

%% @doc start an new redis_client process connect the specify redis server
-spec connect(Host :: inet_host(), Port :: inet_port(), 
        Index :: pos_integer(), Timeout :: timeout()) ->
    {'ok', pid()} | 'ignore' | {'error', any()}.
connect(Host, Port, Index, Timeout) ->
    supervisor:start_child(?CONN_SUP, [{Host, Port}, Index, Timeout]).

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
