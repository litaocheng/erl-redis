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

-export([start/2, start_link/2]).
-export([is_started/0, select_db/1]).
-export([server_list/0, server_type/0]).
-export([get_client_smode/0, get_client/1, get_clients_one/0, get_clients_all/0]).
-export([partition_keys/1, partition_keys/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-compile({inline, [manager_name/1, manager_pid/0, get_client/1]}).

-record(state, {
        type = undefined :: server_type(),  % the servers type
        server = null :: server_info(),     % store the single or dist server
        dbindex = 0,                        % the db index

        dummy
    }).

-define(SERVER, manager_pid()).

%% @doc start the redis_sysdata server
-spec start(Mode :: mode_info(), Passwd :: passwd()) ->
    {'ok', any()} | 'ignore' | {'error', any()}.
start({T, _} = Mode, Passwd) ->
    Name = manager_name(T),
    ?DEBUG2("start ~p mode: ~p passwd :~p", [Name, Mode, Passwd]),
    gen_server:start({local, Name}, ?MODULE, [Mode, Passwd], []).

%% @doc start_link the redis_sysdata server
-spec start_link(Mode :: mode_info(), Passwd :: passwd()) ->
    {'ok', any()} | 'ignore' | {'error', any()}.
start_link({T, _} = Mode, Passwd) ->
    Name = manager_name(T),
    ?DEBUG2("start_link ~p mode: ~p passwd :~p", [Name, Mode, Passwd]),
    gen_server:start_link({local, Name}, ?MODULE, [Mode, Passwd], []).

%% @doc check if the redis_manager is started
-spec is_started() -> boolean().
is_started() ->
    case get_mode() of
        undefined ->
            false;
        _ ->
            true
    end.

%% @doc select db
select_db(_Index) ->
    ok.

%% @doc get the server list info
-spec server_list() ->
    [single_server()].
server_list() ->
    case catch get_server(all) of
        {ok, Servers} when is_list(Servers) ->
            Servers;
        {ok, Server} ->
            [Server];
        {'EXIT', _} ->
            []
    end.

%% @doc get th server config type
-spec server_type() ->
    server_type().
server_type() ->
    gen_server:call(?SERVER, server_type).

%% @doc get a client, the manager must be in single mode
-spec get_client_smode() -> 
    {'ok', server_regname()}.
get_client_smode() ->
    case get_mode() of
        Mode when Mode =/= single ->
            throw({error, redis_mode});
        _ ->
            {ok, [Server]} = get_server(all),
            {ok, random_client(Server)}
    end.

%% @doc get the client according to the Key
-spec get_client(Key :: key()) ->
    {'ok', atom()} | {'error', any()}.
get_client(Key) ->
    {ok, {Host, Port, PoolSize}} = get_server(Key),
    {ok, redis_client:to_regname(Host, Port, random:uniform(PoolSize))}.

%% @doc pick up one connection from the connection pool for each server
-spec get_clients_one() -> [server_regname()].
get_clients_one() ->
    {ok, Servers} = get_server(all),
    {ok, 
    [redis_client:to_regname(Host, Port, random:uniform(PoolSize))
        || {Host, Port, PoolSize} <- Servers]}.

%% @doc get all connections in the conncetion pool for all servers
-spec get_clients_all() -> [server_regname()].
get_clients_all() ->
    {ok, Servers} = get_server(all),
    {ok, 
    [[redis_client:to_regname(Host, Port, I) || I <- lists:seq(1, PoolSize)]
        || {Host, Port, PoolSize} <- Servers]}.

%% @doc partition the keys according to the servers mode
-spec partition_keys(KList :: list()) ->
    [{pid(), [any()]}].
partition_keys(KList) ->
    partition_keys(KList, null).

%% @doc partition the keys according to the servers mode
-spec partition_keys(KList :: list(), KFun :: fun() | null) ->
    [{pid(), [any()]}].
partition_keys(KList, KFun) ->
    % do all the wrok in the outside, because the logic is a bit
    % complicated, we want to block the redis_manager process
    #state{type = Type, server = SigDist}
        = gen_server:call(?SERVER, {get, state}),
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

    Parent = self(),
    Connecter = proc_lib:spawn(
        fun() -> 
            receive 
                {Parent, start} ->
                    Ret = do_setup_connections(Type, SigDist, Passwd),
                    ?DEBUG2("setup connections return is ~p", [Ret]),
                    Parent ! {self(), Ret}
            end
        end),

    Mref = erlang:monitor(process, Connecter),
    Connecter ! {Parent, start},
    receive 
        {Connecter, {ok, _Conns}} ->
            {ok, State};
        {Connecter, {error, _} = Reason} ->
            {stop, Reason};
        {'DOWN', Mref, process, Connecter, Reason} ->
            {stop, Reason}
    end.

handle_call({get, state}, _From, State) ->
    {reply, State, State};
handle_call(server_type, _From, State = #state{type = Type}) ->
    {reply, Type, State};
handle_call({get_server, Type}, _From, State) ->
    Reply = do_get_server(Type, State),
    {reply, Reply, State};
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

%% return the manager registered name
manager_name(single) ->
    ?REDIS_MANAGER_SINGLE;
manager_name(dist) ->
    ?REDIS_MANAGER_DIST.

%% return the manager pid
manager_pid() ->
    case whereis(?REDIS_MANAGER_SINGLE) of
        undefined ->
            whereis(?REDIS_MANAGER_DIST);
        Pid ->
            Pid
    end.

%% random get a client process
random_client({Host, Port, PoolSize}) ->
    redis_client:to_regname(Host, Port, random:uniform(PoolSize)).

%% get server mode
get_mode() ->
    case whereis(?REDIS_MANAGER_SINGLE) of
        undefined ->
            case whereis(?REDIS_MANAGER_DIST) of
                undefined ->
                    undefined;
                Pid when is_pid(Pid) ->
                    dist
            end;
        Pid when is_pid(Pid) ->
            single 
    end.

%% get server info from manager
get_server(Type) ->
    gen_server:call(?SERVER, {get_server, Type}).

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
