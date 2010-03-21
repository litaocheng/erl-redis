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

-export([server_info/0, server_type/0]).
-export([start/0, start_link/0]).
-export([set_passwd/1, set_passwd/2, passwd/1]).
-export([set_mode/1]).
-export([get_client/1, get_all_clients/0]).

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

%% @doc set the server mode
-spec set_mode(Mode :: tuple()) -> 'ok'.
set_mode(Mode) ->
    ?DEBUG2("set servers mode ~p", [Mode]),
    gen_server:call(?SERVER, {set_mode, Mode}).

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

%%
%% gen_server callbacks
%%
init(_Args) ->
    ?DEBUG2("init the redis_servers", []),
    random:seed(now()),
    process_flag(trap_exit, true),
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

%% set the passwd to all the servers
do_set_all_passwd(Passwd, State) ->
    {ok, State#state{auth = [{'$all', Passwd}]}}.

%% set the passwd
do_set_passwd(Server, Passwd, State = #state{auth = Auth}) ->
    Auth2 = lists:keystore(Server, 1, Auth, {Server, Passwd}),
    {ok, State#state{auth = Auth2}}.

%% get the passwd
do_get_passwd(Server, #state{auth = Auth}) ->
    case proplists:get_value(Server, Auth) of
        undefined ->
            proplists:get_value('$all', Auth, "");
        Passwd ->
            Passwd
    end.

%% set the signle mode
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
    {ok, [random_conn(Pids) || {Server, Pids} <- Conns]}.

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


-ifdef(TEST).

passwd_test_() ->
    Host1 = {localhost, 6379},
    Server1 = {localhost, 6379, 2},
    Host2 = {localhost, 6380},
    Server2 = {localhost, 6380, 4},
    Host3 = {localhost, 6381},

    Dist = redis_dist:new([Server1, Server2]),
    State0 = #state{},
    StateS = #state{type = single, server = Server1},
    StateD = #state{type = dist,   server = Dist},
    Pwd = "yes",

    [
        ?_assertEqual({ok, State0#state{auth = [{'$all', Pwd}]}}, 
                do_set_all_passwd(Pwd, State0)),
        ?_assertEqual({ok, StateS#state{auth = [{'$all', Pwd}]}}, 
                do_set_all_passwd(Pwd, StateS)),
        ?_assertEqual({ok, StateD#state{auth = [{'$all', Pwd}]}}, 
                do_set_all_passwd(Pwd, StateD)),

        ?_assertEqual({ok, StateS#state{auth = [{Host1, "pwd"}]}}, do_set_passwd(Host1, "pwd", StateS)),
        ?_assertEqual({ok, StateD#state{auth = [{Host2, "pwd2"}]}}, do_set_passwd(Host2, "pwd2", StateD)),

        ?_assertEqual(Pwd, do_get_passwd(Host1, #state{auth = [{Host1, Pwd}]})),
        ?_assertEqual(Pwd, do_get_passwd(Host2, #state{auth = [{Host1, Pwd}, {Host2, Pwd}]})),
        ?_assertEqual("", do_get_passwd(Host3, #state{auth = []})),
        ?_assertEqual("yes", do_get_passwd(Host3, #state{auth = [{'$all', "yes"}]})),
        ?_assertEqual("pwd1", do_get_passwd(Host3, #state{auth = [{'$all', "yes"}, {Host3, "pwd1"}]}))
    ].


-endif.
