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
-export([set_passwd/1, set_passwd/2, passwd/1]).
-export([single_server/1, dist_server/1]).
-export([get_client/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-record(state, {
        type = undefined :: server_type(),  % the servers type
        auth = [],                          % the auth passwd
        conns = [],                         % the connections
        server = null :: server_info(),     % store the single or dist server

        dummy
    }).

-define(SERVER, ?REDIS_SERVERS).

%% @doc start the redis_sysdata server
-spec start() -> {'ok', any()} | 'ignore' | {'error', any()}.
start() ->
    ?DEBUG2("start ~p", [?SERVER]),
    gen_server:start({local, ?SERVER}, ?MODULE, [], []).

%% @doc start_link the redis_sysdata server
-spec start_link() -> {'ok', any()} | 'ignore' | {'error', any()}.
start_link() ->
    ?DEBUG2("start_link ~p", [?SERVER]),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc set the auth passwd to all the servers
-spec set_passwd(Passwd :: passwd()) -> 'ok'. 
set_passwd(Passwd) ->
    gen_server:call(?SERVER, {set_passwd_all, Passwd}).

%% @doc set the server auth passwd
-spec set_passwd(Server :: server(), Passwd :: passwd()) -> 'ok'. 
set_passwd(Server, Passwd) ->
    gen_server:call(?SERVER, {set_passwd, Server, Passwd}).

%% @doc get the server auth passwd
-spec passwd(Server :: server()) -> 'ok'. 
passwd(Server) ->
    gen_server:call(?SERVER, {passwd, Server}).

%% @doc set the single server info
-spec single_server(SServer :: single_server()) ->
    'ok' | {'error', 'already_set'}.
single_server(SServer) ->
    ?DEBUG2("set single server ~p", [SServer]),
    gen_server:call(?SERVER, {single_server, SServer}).

%% @doc set the dist server info
-spec dist_server(DServer :: dist_server()) ->
    'ok' | {'error', 'already_set'}.
dist_server(DServer) ->
    ?DEBUG2("set dist server: ~p", [redis_dist:to_list(DServer)]),
    gen_server:call(?SERVER, {dist_server, DServer}).

%% @doc get the specify server
-spec get_client(Key :: key()) ->
    {'ok', pid()} | {'error', any()}.
get_client(Key) ->
    gen_server:call(?SERVER, {get_client, Key}).

%%
%% gen_server callbacks
%%
init(_Args) ->
    ?DEBUG2("init the redis_servers", []),
    random:seed(now()),
    {ok, #state{}}.

handle_call({set_passwd_all, Passwd}, _From, State) ->
    {Reply, State2} = do_set_all_passwd(Passwd, State),
    {reply, Reply, State2};
handle_call({set_passwd, Server, Passwd}, _From, State) ->
    {Reply, State2} = do_set_passwd(Server, Passwd, State),
    {reply, Reply, State2};
handle_call({passwd, Server}, _From, State) ->
    Reply = do_get_passwd(Server, State),
    {reply, Reply, State};
handle_call({single_server, SServer}, From, State) ->
    % the do_single_server is async
    State2 = do_single_server(SServer, From, State),
    {noreply, State2};
handle_call({dist_server, DServer}, From, State) ->
    % the do_dist_server is async
    State2 = do_dist_server(DServer, From, State),
    {noreply, State2};
handle_call({get_client, Key}, From, State) ->
    do_get_client(Key, From, State),
    {noreply, State};
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

%% set the passwd to all the servers
do_set_all_passwd(Passwd, 
        State = #state{type = single, server = {Host, Port, _}}) ->
    {ok, State#state{auth = [{{Host, Port}, Passwd}]}};
do_set_all_passwd(Passwd,
        State = #state{type = dist, server = DServer}) ->
    Servers = redis_dist:server_list(DServer),
    {ok, State#state{auth = [{{Host, Port}, Passwd} || {Host, Port, _} <- Servers]}}.

%% set the passwd
do_set_passwd(Server, Passwd, State = #state{auth = Auth}) ->
    Auth2 = lists:keystore(Server, 1, Auth, {Server, Passwd}),
    {ok, State#state{auth = Auth2}}.

%% get the passwd
do_get_passwd(Server, #state{auth = Auth}) ->
    proplists:get_value(Server, Auth, "").

%% set the signle server info
do_single_server(SServer, From,
        State = #state{type = undefined, conns = []}) ->
    % setup the connections in new process
    setup_connections([SServer], From),
    % update the server field
    State#state{type = single, server = SServer};
do_single_server(_SServer, From, State = #state{type = Type}) ->
    ?ERROR2("you'd set server info, type:~p!", [Type]),
    gen_server:reply(From, {error, already_set}),
    State.

%% set the dist server info
do_dist_server(DServer, From, State = #state{type = undefined}) ->
    % setup the connections in new process
    setup_connections(redis_dist:to_list(DServer), From),
    % update the server field
    State#state{type = dist, server = DServer};
do_dist_server(_DServer, From, State = #state{type = Type}) ->
    ?ERROR2("you'd set server info, type:~p!", [Type]),
    gen_server:reply(From, {error, already_set}),
    State.

%% get the client
do_get_client(_Key, From, #state{type = single, conns = Conns}) ->
    ?DEBUG2("(single)get client for key: ~p", [_Key]),
    case is_conn_empty(Conns) of
        true ->
            gen_server:reply(From, {error, no_server});
        false ->
            Conn = get_conn(Conns),
            gen_server:reply(From, {ok, Conn})
    end;
do_get_client(Key, From, #state{type = dist, conns = Conns, server = DServer}) ->
    {ok, Server} = redis_dist:get_server(Key, DServer),
    ?DEBUG2("(dist)get client with server:~p for key:~p ", [Server, Key]),
    case find_conn(Server, Conns) of
        none ->
            gen_server:reply(From, {error, no_server});
        {ok, Conn} ->
            gen_server:reply(From, {ok, Conn})
    end.

%% set the client info
do_set_client(Server, Index, Pid, State = #state{type = single, conns = Conns}) ->
    ?DEBUG2("(single)set the redis client info, server:~p (~p) pid:~p", [Server, Index, Pid]),
    State#state{conns = set_conn(Server, Index, Pid, Conns)};
do_set_client(Server, Index, Pid, State = #state{type = dist, conns = Conns}) ->
    ?DEBUG2("(dist)set the redis client info, server:~p (~p) pid:~p", [Server, Index, Pid]),
    State#state{conns = set_conn(Server, Index, Pid, Conns)}.

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

%% spawn a new process to call the redis_conn_sup:connect/1 function.
%% thus, we will not block the redis_servers process. when the redis_client
%% process created by redis_conn_sup connect the redis server ok, 
%% it will notify the redis_servers to update the conns info
setup_connections(Servers, From) ->
    F = fun() -> 
        ?DEBUG2("setup the connections ~p", [Servers]),
        Errors =
        lists:foldl(
            fun({Host, Port, Pool}, Acc) ->
                case catch connect_server(Host, Port, Pool) of
                    ok ->
                        Acc;
                    {error, Reason} ->
                        [{{Host, Port}, {error, Reason}} | Acc]
                end
            end,
        [], Servers),

        Reply = 
        case Errors of
            [] ->
                ok;
            [_|_] ->
                Errors
        end,
        gen_server:reply(From, Reply)
    end,
    proc_lib:spawn(F),
    ok.

%% connect one server
connect_server(Host, Port, Pool) ->
   [begin 
        case redis_conn_sup:connect(Host, Port, N) of
            {error, Reason} ->
                ?ERROR2("make the ~p connection to redis server ~p:~p error:~p", 
                    [N, Host, Port, Reason]),
                throw({error, Reason});
            {ok, _Client} ->
                ok
        end
    end || N <- lists:seq(1, Pool)],
    ok.


-ifdef(TEST).


-endif.
