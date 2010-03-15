%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng <litaocheng@gmail.com>
%%% @doc the redis servers manager
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis_servers).
-author('litaocheng@gmail.com').
-vsn('0.1').
-behaviour(gen_server).
-include("redis.hrl").

-export([start/0, start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-record(state, {
    }).

-define(SERVER, ?MODULE).

%% @doc start the redis_sysdata server
-spec start() -> {'ok', any()} | 'ignore' | {'error', any()}.
start() ->
    ?DEBUG2("start ~p ~n", [?SERVER]),
    gen_server:start({local, ?SERVER}, ?MODULE, [], []).

%% @doc start_link the redis_sysdata server
-spec start_link() -> {'ok', any()} | 'ignore' | {'error', any()}.
start_link() ->
    ?DEBUG2("start_link ~p ~n", [?SERVER]),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%
%% gen_server callbacks
%%
init(_Args) ->
    {ok, #state{}}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_Old, State, _Extra) ->
    {ok, State}.

%%-----------------------------------------------------------------------------
%%
%% internal API
%%
%%-----------------------------------------------------------------------------

-ifdef(TEST).

-endif.
