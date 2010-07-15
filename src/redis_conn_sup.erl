%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litao cheng <litaocheng@gmail.com>
%%% @doc redis client connection supervisor
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis_conn_sup).
-author('litaocheng@gmail.com').
-vsn('0.1').
-include("redis_internal.hrl").

-behaviour(supervisor).

-export([start_link/0]).
-export([connect/3, connect/4]).

%% some auxiliary functions
-export([sup_spec/0
        , sup_start_client/3, sup_start_client/4
        , sup_rand_client/0, sup_rand_client/2, sup_rand_client/3]).

%% supervisor callback
-export([init/1]).

%% @doc the application start callback
-spec start_link() -> 
    {'ok', pid()} | 'ignore' | {'error', any()}.
start_link() ->
    ?INFO2("start the supervisor", []),
    supervisor:start_link({local, ?CONN_SUP}, ?MODULE, []).

%% @doc start an new redis_client process connect the specify redis server
connect(Host, Port, Passwd) ->
    supervisor:start_child(?CONN_SUP, [Host, Port, Passwd]).

%% @doc start an new redis_client process connect the specify redis server
connect(Host, Port, Passwd, Name) ->
    supervisor:start_child(?CONN_SUP, [Host, Port, Passwd, Name]).

%% @doc return the specification as the child of other supervisor
sup_spec() ->
    {redis_client_sup, {redis_conn_sup, start_link, []},
            permanent, 1000, supervisor, [redis_client]}.


%% @doc start client to Host:Port with a connection pool, in the ?CONN_SUP
sup_start_client(Host, Port, Pass) ->
    sup_start_client(Host, Port, Pass, ?CONN_POOL_DEF).

sup_start_client(Host, Port, Pass, Pool) when is_integer(Pool) ->
    [begin
        Name = redis_client:name(Host, Port, I),
        {ok, _} = connect(Host, Port, Pass, Name)
    end || I <- lists:seq(1, Pool)].

%% @doc random select one connection form the ?CONN_SUP
sup_rand_client() ->
    Children = supervisor:which_children(?CONN_SUP),
    Len = length(Children),
    {_Id, Child, worker, _Modules} = lists:nth(random:uniform(Len), Children),
    redis_client:handler(Child).

%% @doc random select one connection with Host:Port from the connection pool
%%      return the parameterized redis module
sup_rand_client(Host, Port) ->
    sup_rand_client(Host, Port, ?CONN_POOL_DEF).

sup_rand_client(Host, Port, Pool) ->
    Selected = redis_client:existing_name(Host, Port, random:uniform(Pool)),
    redis_client:handler(Selected).

%% @doc the connection supervisor callback
init([]) -> 
    ?DEBUG2("init supervisor", []),
    Stragegy = {simple_one_for_one, 100000, 60},
    Client = {undefined, {redis_client, start_link, []},
                permanent, 1000, worker, [redis_client]},

    {ok, {Stragegy, [Client]}}.

%%
%% internal API
%%
