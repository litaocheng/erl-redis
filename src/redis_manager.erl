%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng <litaocheng@gmail.com>
%%% @doc the redis servers manager
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis_manager).
-author('litaocheng@gmail.com').
-vsn('0.1').
-behaviour(gen_server).
-include("redis_internal.hrl").


-export([start/3, start_link/3]).
-export([set_selected_db/2, get_selected_db/1]).
-export([server_list/1, server_type/1]).
-export([get_client/2, get_clients/1]).
-export([partition_keys/2, partition_keys/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-compile({inline, [get_client/2]}).

-record(state, {
        type = undefined :: server_type(),  % the servers type
        server = null :: server_info(),     % store the single or dist server
        dbindex = 0,                        % the db index

        dummy
    }).

%% @doc start the redis_sysdata server
-spec start(atom(), mode_info(), passwd()) ->
    {'ok', any()} | 'ignore' | {'error', any()}.
start(Name, Mode, Passwd) ->
    ?DEBUG2("start ~p mode: ~p passwd :~p", [Name, Mode, Passwd]),
    gen_server:start({local, Name}, ?MODULE, [Mode, Passwd], []).

%% @doc start_link the redis_sysdata server
-spec start_link(atom(), mode_info(), passwd()) ->
    {'ok', any()} | 'ignore' | {'error', any()}.
start_link(Name, Mode, Passwd) ->
    ?DEBUG2("start_link *~p* mode: ~p passwd :~p", [Name, Mode, Passwd]),
    gen_server:start_link({local, Name}, ?MODULE, [Mode, Passwd], []).

%% @doc set the select db
-spec set_selected_db(atom(), index()) -> 
    'ok' | {'error', any()}.
set_selected_db(Mgr, Index) ->
    gen_server:call(Mgr, {set, dbindex, Index}).

%% @doc get the selected db
-spec get_selected_db(atom()) ->
    index().
get_selected_db(Mgr) ->
    gen_server:call(Mgr, {get, dbindex}).

%% @doc get the server list 
-spec server_list(Mgr :: atom()) ->
    [single_server()].
server_list(Mgr) ->
    case catch get_server(Mgr, all) of
        {ok, Servers} when is_list(Servers) ->
            Servers;
        {ok, Server} ->
            [Server];
        {'EXIT', _} ->
            []
    end.

%% @doc get the server config type
-spec server_type(Mgr :: atom()) ->
    server_type().
server_type(Mgr) ->
    gen_server:call(Mgr, server_type).

%% @doc get the client according to the Key
-spec get_client(atom(), key()) ->
    {'ok', atom()} | {'error', any()}.
get_client(Mgr, Key) ->
    {ok, Server} = get_server(Mgr, Key),
    {ok, random_client(Server)}.

%% @doc pick up one connection from the connection pool for each server
-spec get_clients(atom()) -> [atom()].
get_clients(Mgr) ->
    {ok, Servers} = get_server(Mgr, all),
    {ok, 
    [redis_client:to_regname(Host, Port, random:uniform(PoolSize))
        || {Host, Port, PoolSize} <- Servers]}.

%% @doc partition the keys according to the servers mode
-spec partition_keys(atom(), KList :: list()) ->
    [{pid(), [any()]}].
partition_keys(Mgr, KList) ->
    partition_keys(Mgr, KList, null).

%% @doc partition the keys according to the servers mode
-spec partition_keys(atom(), KList :: list(), KFun :: fun() | null) ->
    [{pid(), [any()]}].
partition_keys(Mgr, KList, KFun) ->
    % do all the wrok in the outside, because the logic is a bit
    % complicated, we want to block the redis_manager process
    #state{type = Type, server = SigDist}
        = gen_server:call(Mgr, {get, state}),
    case Type of
        single ->
            [{random_client(SigDist), KList}];
        dist ->
            ServerKeys = redis_dist:partition_keys(KList, KFun, SigDist),
            ?DEBUG2("server keys pairs are ~p", [ServerKeys]),
            [{random_client(Server), KLpart} 
                || {Server, KLpart} <- ServerKeys]
    end.

                
%%
%% gen_server callbacks
%%
init([{Type, SigDist}, Passwd]) ->
    ?DEBUG2("init the redis manager", []),
    random:seed(now()),
    process_flag(trap_exit, true),
    State = 
        #state{type = Type,
                server = SigDist},
    case do_setup_connections(Type, SigDist, Passwd) of
        {ok, _Conns} ->
            {ok, State};
        {error, _} = Reason ->
            {stop, Reason}
    end.

handle_call({get, state}, _From, State) ->
    {reply, State, State};
handle_call(server_type, _From, State = #state{type = Type}) ->
    {reply, Type, State};
handle_call({get_server, Type}, _From, State) ->
    Reply = do_get_server(Type, State),
    {reply, Reply, State};
handle_call({set, dbindex, Index}, _From, State) ->
    {reply, ok, State#state{dbindex = Index}};
handle_call({get, dbindex}, _From, State = #state{dbindex = Index}) ->
    {reply, Index, State};
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

%% random get a client process
random_client({Host, Port, PoolSize}) ->
    redis_client:to_regname(Host, Port, random:uniform(PoolSize)).

%% get server info from manager
get_server(Mgr, Type) ->
    gen_server:call(Mgr, {get_server, Type}).

%% do get server
do_get_server(all, #state{type = single, server = SigDist}) ->
    {ok, [SigDist]};
do_get_server(_Key, #state{type = single, server = SigDist}) ->
    {ok, SigDist};
do_get_server(all, #state{type = dist, server = SigDist}) ->
    {ok, redis_dist:to_list(SigDist)};
do_get_server(Key, #state{type = dist, server = SigDist}) ->
    redis_dist:get_server(Key, SigDist).

%% return random item
%random([H]) -> % only one
%    H;
%andom([_|_] = L) ->
%   lists:nth(random:uniform(length(L)), L).

%% setup connections
do_setup_connections(Type, SigDist, Passwd) ->
    Servers = 
    case Type of
        single ->
            [SigDist];
        dist ->
            redis_dist:to_list(SigDist)
    end,
    ?DEBUG2("setup the connections to ~p", [Servers]),
    {Conns, Errors} =
    lists:foldl(
        fun({Host, Port, Pool}, {ConnAcc, BadAcc}) ->
            case catch do_setup_pool(Host, Port, Pool, Passwd) of
                {error, Reason} ->
                    {ConnAcc, [{{Host, Port}, Reason} | BadAcc]};
                ConnPool ->
                    {[{{Host, Port}, ConnPool} | ConnAcc], BadAcc}
            end
        end,
    {[], []}, Servers),

    case Errors of
        [] ->
            {ok, Conns};
        [_|_] ->
            {error, Errors}
    end.

%% setup connection pool with one server
do_setup_pool(Host, Port, Pool, Passwd) ->
    [begin 
            case redis_conn_sup:connect(Host, Port, Index, ?CONN_TIMEOUT, Passwd) of
                {error, Reason} ->
                    ?ERROR2("make the ~p connection to redis server ~p:~p error:~p", 
                        [Index, Host, Port, Reason]),
                    throw({error, Reason});
                {ok, Client} ->
                    {Index, Client}
                     
            end
    end || Index <- lists:seq(1, Pool)].

-ifdef(TEST).

-endif.
