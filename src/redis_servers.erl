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
-export([set_server/2]).
-export([get_client/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-record(state, {
        type = undefined :: server_type(),  % the servers type
        auth = [],                          % the auth passwd
        conns = [],                         % the connections
        server = null,                      % store the single server
        dist = null,                        % store the dist info

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
-spec set_passwd(Server :: single_server(), Passwd :: passwd()) -> 'ok'. 
set_passwd(Server, Passwd) ->
    gen_server:call(?SERVER, {set_passwd, Server, Passwd}).

%% @doc get the server auth passwd
-spec passwd(Server :: single_server()) -> 'ok'. 
passwd(Server) ->
    gen_server:call(?SERVER, {passwd, Server}).

%% @doc set the server info
-spec set_server(Type :: server_type(), Server :: server_info()) ->
    'ok' | {'error', 'already_set'}.
set_server(Type, Server) ->
    ?DEBUG2("set server ~p ~p", [Type, Server]),
    gen_server:call(?SERVER, {set_server, Type, Server}).

%% @doc get the specify server
-spec get_client(Key :: key()) ->
    {'ok', connection()} | {'error', any()}.
get_client(Key) ->
    gen_server:call(?SERVER, {get_client, Key}).

%%
%% gen_server callbacks
%%
init(_Args) ->
    ?DEBUG2("init the redis_servers", []),
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
handle_call({set_server, Type, Server}, _From, State) ->
    {Reply, State2} = do_set_server(Type, Server, State),
    {reply, Reply, State2};
handle_call({get_client, Key}, From, State) ->
    case do_get_client(Key, From, State) of
        {Reply, State2} ->
            {reply, Reply, State2};
        State2 -> 
            {noreply, State2}
    end;
handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast({set_client, Server, Pid, Sock}, State) ->
    % caller is the function set_client in redis_client
    State2 = do_set_client(Server, Pid, Sock, State),
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
        State = #state{type = single, server = Server}) ->
    {ok, State#state{auth = [{Server, Passwd}]}};
do_set_all_passwd(Passwd,
        State = #state{type = dist, dist = Dist}) ->
    Servers = redis_dist:server_list(Dist),
    {ok, State#state{auth = [{S, Passwd} || S <- Servers]}}.

%% set the passwd
do_set_passwd(Server, Passwd, State = #state{auth = Auth}) ->
    Auth2 = lists:keystore(Server, 1, Auth, {Server, Passwd}),
    {ok, State#state{auth = Auth2}}.

%% get the passwd
do_get_passwd(Server, #state{auth = Auth}) ->
    proplists:get_value(Server, Auth, "").

%% set the server info
do_set_server(single, Server = {_Host, _Port}, 
        State = #state{type = undefined, conns = [], server = null}) ->
    {ok, State#state{type = single, server = Server}};
do_set_server(single, _Server, State = #state{type = Type}) ->
    ?ERROR2("you'd set server info, type:~p!", [Type]),
    {{error, already_set}, State};
do_set_server(dist, Dist, State = #state{type = undefined}) ->
    {ok, State#state{type = dist, conns = [], dist = Dist}};
do_set_server(dist, _Dist, State = #state{type = Type}) ->
    ?ERROR2("you'd set server info, type:~p!", [Type]),
    {{error, already_set}, State}.

%% get the client
do_get_client(_Key, From, State = #state{type = single, conns = Conns, server = Server}) ->
    ?DEBUG2("(single)get client for key: ~p", [_Key]),
    case Conns of
        [{_Server, Conn}] ->
            {Conn, State};
        [] ->
            ok = start_client(From, Server),
            State
    end;
do_get_client(Key, From, State = #state{type = dist, conns = Conns, dist = Dist}) ->
    {ok, Server} = redis_dist:get_server(Key, Dist),
    ?DEBUG2("(dist)get client with server:~p for key:~p ", [Server, Key]),
    case find_conn(Server, Conns) of
        none ->
            ok = start_client(From, Server),
            State;
        {ok, Conn} ->
            {Conn, State}
    end.

%% set the client info
do_set_client(Server, Pid, Sock, State = #state{type = single}) ->
    ?DEBUG2("(single)set the redis client info, server:~p pid:~p sock:~p", [Server, Pid, Sock]),
    State#state{conns = [{Server, {Pid, Sock}}]};
do_set_client(Server, Pid, Sock, State = #state{type = dist, conns = Conns}) ->
    ?DEBUG2("(dist)set the redis client info, server:~p pid:~p sock:~p", [Server, Pid, Sock]),
    Conns = lists:keystore(Server, 1, Conns, {Server, {Pid, Sock}}),
    State#state{conns = Conns}.

%% find connection from the conns list
find_conn(Server, Conns) ->
    case lists:keyfind(Server, 1, Conns) of
        false ->
            none;
        {Server, Conn} ->
            {ok, Conn}
    end.

%% spawn a new process to call the redis_conn_sup:connect/1 function.
%% thus, we will not block the redis_servers process. when the redis_client
%% process created by redis_conn_sup connect the redis server ok, 
%%% it will notify the redis_servers to update the conns info
start_client(From, Server = {Host, Port}) ->
    spawn(
        fun() ->
            {ok, Client} = redis_conn_sup:connect(Host, Port),
            {ok, Sock} = redis_client:get_sock(Client),
            ?DEBUG2("connect the server:~p ok, now reply the caller", [Server]),
            gen_server:reply(From, {ok, {Client, Sock}})
        end),
    ok.

-ifdef(TEST).


-endif.
