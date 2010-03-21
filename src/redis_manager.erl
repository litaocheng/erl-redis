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
-include("redis.hrl").

-export([start/2, start_link/2]).
-export([server_info/0, server_type/0]).
-export([get_client/1, get_all_clients/0]).
-export([partition_keys/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-record(state, {
        type = undefined :: server_type(),  % the servers type
        conns = [],                         % the connections
        server = null :: server_info(),     % store the single or dist server

        dummy
    }).

-define(SERVER, ?REDIS_MANAGER).

%% @doc start the redis_sysdata server
-spec start(Mode :: mode_info(), Passwd :: passwd()) ->
    {'ok', any()} | 'ignore' | {'error', any()}.
start(Mode, Passwd) ->
    ?DEBUG2("start ~p mode: ~p passwd :~p", [?SERVER, Mode, Passwd]),
    gen_server:start({local, ?SERVER}, ?MODULE, [Mode, Passwd], []).

%% @doc start_link the redis_sysdata server
-spec start_link(Mode :: mode_info(), Passwd :: passwd()) ->
    {'ok', any()} | 'ignore' | {'error', any()}.
start_link(Mode, Passwd) ->
    ?DEBUG2("start_link ~p mode: ~p passwd :~p", [?SERVER, Mode, Passwd]),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Mode, Passwd], []).

%% @doc get the server list info
-spec server_info() ->
    [single_server()].
server_info() ->
    gen_server:call(?SERVER, server_info).

%% @doc get th server config type
-spec server_type() ->
    server_type().
server_type() ->
    gen_server:call(?SERVER, server_type).

%% @doc get the client
-spec get_client(Key :: key()) ->
    {'ok', pid()} | {'error', any()}.
get_client(Key) ->
    gen_server:call(?SERVER, {get_client, Key}).

%% @doc get all the clients
-spec get_all_clients() ->
    {ok, [pid()]}.
get_all_clients() ->
    gen_server:call(?SERVER, get_all_clients).

%% @doc partition the keys according to the servers mode
-spec partition_keys(KList :: list(), KFun :: fun()) ->
    [{pid(), [any()]}].
partition_keys(_KList, _KFun) ->
    ok.

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

handle_call(server_info, _From, State) ->
    Reply = do_server_info(State),
    {reply, Reply, State};
handle_call(server_type, _From, State = #state{type = Type}) ->
    {reply, Type, State};
handle_call({set_mode, {Mode, Servers}}, _From, State) ->
    {Reply, State2} = do_set_mode(Mode, Servers, State),
    {reply, Reply, State2};
handle_call({get_client, Key}, _From, State) ->
    Reply = do_get_client(Key, State),
    {reply, Reply, State};
handle_call(get_all_clients, _From, State) ->
    Reply = do_get_all_clients(State),
    {reply, Reply, State};
handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast({set_client, Server, Index, Pid}, State) ->
    % caller is the function set_client in redis_client
    State2 = do_set_client(Server, Index, Pid, State),
    ?DEBUG2("conns is ~p", [State2#state.conns]),
    {noreply, State2};
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

%% set the single mode
do_set_mode(Mode, Servers, State = #state{type = undefined}) ->
    {ok, State#state{type = Mode, server = Servers}}.

%% get the client
do_get_client(_Key, #state{type = single, conns = Conns}) ->
    ?DEBUG2("(single) get client for key: ~p", [_Key]),
    case is_conn_empty(Conns) of
        true ->
            {error, no_server};
        false ->
            {ok, get_conn(Conns)}
    end;
do_get_client(Key, #state{type = dist, conns = Conns, server = DServer}) ->
    {ok, Server} = redis_dist:get_server(Key, DServer),
    ?DEBUG2("(dist) get client with server:~p for key:~p ", [Server, Key]),
    case find_conn(Server, Conns) of
        none ->
            {error, no_server};
        {ok, Conn} ->
            {ok, Conn}
    end.

%% get all the clients
do_get_all_clients(#state{type = single, conns = Conns}) ->
    {ok, [random_conn(Pids) || {_Server, Pids} <- Conns]}.

%% set the client info
do_set_client(Server, Index, Pid, State = #state{type = single, conns = Conns}) ->
    ?DEBUG2("(single)set the redis client info, server:~p (~p) pid:~p", [Server, Index, Pid]),
    State#state{conns = set_conn(Server, Index, Pid, Conns)};
do_set_client(Server, Index, Pid, State = #state{type = dist, conns = Conns}) ->
    ?DEBUG2("(dist)set the redis client info, server:~p (~p) pid:~p", [Server, Index, Pid]),
    State#state{conns = set_conn(Server, Index, Pid, Conns)}.

%% get server info
do_server_info(#state{type = Type, server = S}) ->
    case Type of
        single ->
            S;
        dist ->
            motown_dist:to_list(S);
        undefined ->
            []
    end.

%%
%% about conns
%% conns is an list, the element struct is 
%% {Server, [{Index, Pid}]}
%% Server is {Host, Port}
%% Index is the pool index
%% Pid is the redis_client pid
%%

%% is the conns is empty?
is_conn_empty(Conns) ->
    Conns =:= [].

%% get one connection from the conns
get_conn([]) -> none;
get_conn([{_Server, Pids} | _]) ->
    random_conn(Pids).

%% random get one connection
random_conn([{_Index, Pid}]) -> % onle one
    Pid;
random_conn(Pids) ->
    {_Index, Pid} = lists:nth(random:uniform(length(Pids)), Pids),
    Pid.
    
%% find connection from the conns 
find_conn(Server, Conns) ->
    case lists:keyfind(Server, 1, Conns) of
        false ->
            none;
        {Server, Pids} ->
            random_conn(Pids)
    end.

%% set connection to the conns
set_conn(Server, Index, Pid, Conns) ->
    case lists:keyfind(Server, 1, Conns) of
        false -> % not found
            [{Server, [{Index, Pid}]} | Conns];
        {Server, Pids} ->
            Pids2 = lists:keystore(Index, 1, Pids, {Index, Pid}),
            lists:keyreplace(Server, 1, Conns, {Server, Pids2})
    end.

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
