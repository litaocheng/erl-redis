%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng <litaocheng@gmail.com>
%%% @doc the redis client
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis_client).
-author('litaocheng@gmail.com').
-vsn('0.1').
-behaviour(gen_server).
-include("redis.hrl").
-export([start_link/3]).
-export([get_sock/1]).
-export([send/2, multi_send/2, multi_send/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-record(state, {
        server,             % the host and port
        index,              % the index in connection pool
        sock = null,        % the socket
        db = 0,             % the default db index
        dummy
    }).

-define(TCP_OPTS, [inet, binary, {active, false}, 
            {packet, line}, {nodelay, true},
            {send_timeout, 5000}, {send_timeout_close, true}]).


%% @doc start_link the redis_client server
-spec start_link(Server :: single_server(), Index :: pos_integer(), Timeout :: timeout()) -> 
    {'ok', any()} | 'ignore' | {'error', any()}.
start_link(Server, Index, Timeout) ->
    ?DEBUG2("start_link redis_client ~p (~p)", [Server, Index]),
    gen_server:start_link(?MODULE, {Server, Index, Timeout}, []).

%% @doc return the socket
-spec get_sock(Client :: pid()) -> {'ok', port()}.
get_sock(Client) ->
    {ok, gen_server:call(Client, get_sock)}.

%% @doc send the data
-spec send(Client :: pid(), Data :: iolist()) -> any().
send(Client, Data) ->
    ?DEBUG2("send data ~p to server ~p", [Data, get_server(Client)]),
    gen_server:call(Client, {command, Data}).

%% @doc send to multi clients
-spec multi_send(Clients :: [pid()], Data :: iolist()) ->
    {[{pid(), any()}], [pid()]}.
multi_send(Clients, Data) when is_list(Clients) ->
    do_multi_send(Clients, {command, Data}, infinity).

-spec multi_send(Clients :: [pid()], Data :: iolist(), Timeout :: timeout()) ->
    {[{pid(), any()}], [pid()]}.
multi_send(Clients, Data, Timeout) when is_list(Clients) ->
    do_multi_send(Clients, {command, Data}, Timeout).

%%
%% gen_server callbacks
%%
init({Server = {Host, Port}, Index, Timeout}) ->
    ?DEBUG2("init the redis client ~p:~p (~p)", [Host, Port, Index]),
    case gen_tcp:connect(Host, Port, ?TCP_OPTS, Timeout) of
        {ok, Sock} ->
            case do_auth(Sock, Server) of
                ok ->
                    % notify the client info to the redis_servers
                    ok = set_client(Server, Index, self()),

                    {ok, #state{server = Server, index = Index, sock = Sock}};
                {tcp_error, Reason} ->
                    {stop, Reason};
                _ ->
                    ?ERROR2("auth failed", []),
                    {stop, auth_failed}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({command, Data}, _From, State = #state{sock = Sock, server = Server}) ->
    case do_send_recv(Data, Sock, Server) of
        {tcp_error, Reason} ->
            {stop, Reason, {tcp_error, Reason}, State};
        Reply ->
            {reply, Reply, State}
    end;
handle_call(get_server, _From, State = #state{server = Server}) ->
    {reply, Server, State};
handle_call(get_sock, _From, State = #state{sock = Sock}) ->
    {reply, Sock, State};
handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({error, timeout}, State) -> % send timeout
    ?ERROR2("send timeout, the socket closed, process will restart", []),
    {stop, {error, timeout}, State};
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

%% do sync send and recv 
do_send_recv(Data, Sock, Server) ->
    case gen_tcp:send(Sock, Data) of
        ok -> % receive response
            case gen_tcp:recv(Sock, 0, ?RECV_TIMEOUT) of
                {ok, Packet} ->
                    ?DEBUG2("recv reply :~p", [Packet]),
                    redis_proto:parse_reply(Packet, Sock);
                {error, Reason} ->
                    ?ERROR2("recv message from ~p error:~p", [Server, Reason]),
                    {tcp_error, Reason}
            end;
        {error, Reason} ->
            ?ERROR2("send message to ~p error:~p", [Server, Reason]),
            {tcp_error, Reason}
    end.

%% do the auth
do_auth(Sock, Server) ->
    Passwd = redis_servers:passwd(Server),
    ?DEBUG2("auth to server ~p pwd:~p", [Server, Passwd]),
    case Passwd of
        "" ->
            ok;
        _ ->
            do_send_recv([<<"AUTH ">>, Passwd, ?CRLF], Sock, Server)
    end.

%% notify the redis_servers the client info
set_client(Server, Index, Pid) ->
    gen_server:cast(?REDIS_SERVERS, {set_client, Server, Index, Pid}).

%% get the server info
get_server(Client) ->
    gen_server:call(Client, get_server).

%%
%% about multi send
%%

%% do the multiple send to the clients
do_multi_send(Clients, Req, infinity) ->
    Tag = make_ref(),
    Monitors = do_send_reqs(Clients, Tag, Req),
    do_rec_replys(Tag, Monitors, undefined);
do_multi_send(Clients, Req, Timeout) ->
    Tag = make_ref(),
    Caller = self(),
    Receiver =
	spawn(
    fun() ->
        %% Middleman process. Should be unsensitive to regular
        %% exit signals. The sychronization is needed in case
        %% the receiver would exit before the caller started
        %% the monitor.
        process_flag(trap_exit, true),
        Mref = erlang:monitor(process, Caller),
        receive
          {Caller,Tag} ->
              Monitors = do_send_reqs(Clients, Tag, Req),
              TimerId = erlang:start_timer(Timeout, self(), ok),
              Result = do_rec_replys(Tag, Monitors, TimerId),
              exit({self(),Tag,Result});
          {'DOWN',Mref,_,_,_} ->
              %% Caller died before sending us the go-ahead.
              %% Give up silently.
              exit(normal)
        end
    end),
    Mref = erlang:monitor(process, Receiver),
    Receiver ! {self(),Tag},
    receive
        {'DOWN',Mref,_,_,{Receiver,Tag,Result}} ->
            Result;
        {'DOWN',Mref,_,_,Reason} ->
            %% The middleman code failed. Or someone did 
            %% exit(_, kill) on the middleman process => Reason==killed
            exit(Reason)
    end.

%% send the requests to all the process
do_send_reqs(Clients, Tag, Req) ->
    do_send_reqs(Clients, Tag, Req, []).
do_send_reqs([Pid|Tail], Tag, Req, Monitors) when is_pid(Pid) ->
    Monitor = erlang:monitor(process,Pid),
    catch Pid ! {'$gen_call', {self(), Tag}, Req},
    do_send_reqs(Tail, Tag, Req, [{Pid, Monitor} | Monitors]);
do_send_reqs([], _Tag, _Req, Monitors) -> 
    Monitors.

%% receive the replys from clients
do_rec_replys(Tag, Monitors, TimerId) ->
    do_rec_replys(Tag, Monitors, [], [], TimerId).
do_rec_replys(Tag, [{P, R}|Tail], BadClients, Replies, TimerId) ->
    receive
	{'DOWN', R, _, _, _} ->
	    do_rec_replys(Tag, Tail, [P|BadClients], Replies, TimerId);
	{Tag, Reply} ->  %% Tag is bound !!!
	    unmonitor(R), 
	    do_rec_replys(Tag, Tail, BadClients, 
		      [{P,Reply}|Replies], TimerId);
	{timeout, TimerId, _} ->	
	    unmonitor(R),
	    %% Collect all replies that already have arrived
	    do_rec_replys_rest(Tag, Tail, [P|BadClients], Replies)
    end;
do_rec_replys(_, [], BadClients, Replies, TimerId) ->
    case catch erlang:cancel_timer(TimerId) of
	false ->  % It has already sent it's message
	    receive
		{timeout, TimerId, _} -> ok
	    after 0 ->
		    ok
	    end;
	_ -> % Timer was cancelled, or TimerId was 'undefined'
	    ok
    end,
    {Replies, BadClients}.
%% Collect all replies that already have arrived
do_rec_replys_rest(Tag, [{P,R}|Tail], BadClients, Replies) ->
    receive
	{'DOWN', R, _, _, _} ->
	    do_rec_replys_rest(Tag, Tail, [P|BadClients], Replies);
	{Tag, Reply} -> %% Tag is bound !!!
	    unmonitor(R),
	    do_rec_replys_rest(Tag, Tail, BadClients, [{P,Reply}|Replies])
    after 0 ->
	    unmonitor(R),
	    do_rec_replys_rest(Tag, Tail, [P|BadClients], Replies)
    end;
do_rec_replys_rest(_Tag, [], BadClients, Replies) ->
    {Replies, BadClients}.

%% Cancels a monitor started with Ref=erlang:monitor(_, _).
unmonitor(Ref) when is_reference(Ref) ->
    erlang:demonitor(Ref),
    receive
	{'DOWN', Ref, _, _, _} ->
	    true
    after 0 ->
	    true
    end.


-ifdef(TEST).

-endif.
