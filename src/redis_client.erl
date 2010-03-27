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
-include("redis_internal.hrl").

-export([start_link/4]).
-export([to_regname/3, get_server/1, get_sock/1]).
-export([send/2, multi_send/2, multi_send/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-record(state, {
        server,             % the host and port
        index,              % the index in connection pool
        sock = null,        % the socket
        db = 0              % the default db index
    }).

-define(TCP_OPTS, [inet, binary, {active, false}, 
            {packet, line}, {nodelay, true},
            {recbuf, 102400},
            {sndbuf, 102400},
            {send_timeout, 5000}, {send_timeout_close, true}]).

%% @doc start_link the redis_client server
-spec start_link(Server :: inet_server(), Index :: pos_integer(), 
    Timeout :: timeout(), Passwd :: passwd()) -> 
    {'ok', any()} | 'ignore' | {'error', any()}.
start_link({Host, Port} = Server, Index, Timeout, Passwd) ->
    Name = to_regname(Host, Port, Index, true),
    ?DEBUG2("start_link redis_client ~p", [Name]),
    gen_server:start_link({local, Name}, ?MODULE, {Server, Index, Timeout, Passwd}, []).

%% @doc convert server struct to registered process name
-spec to_regname(Host :: inet_host(), Port :: inet_port(), Index :: index()) ->
    atom().
to_regname(Host, Port, Index) ->
    to_regname(Host, Port, Index, false).

%% @doc get the server info
-spec get_server(Client :: pid()) -> inet_server().
get_server(Client) ->
    gen_server:call(Client, get_server).

%% @doc return the socket
-spec get_sock(Client :: pid()) -> {'ok', port()}.
get_sock(Client) ->
    {ok, gen_server:call(Client, get_sock)}.

%% @doc send the data
-spec send(Client :: pid(), Data :: iolist()) -> any().
send(Client, Data) ->
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
init({Server = {Host, Port}, Index, Timeout, Passwd}) ->
    ?DEBUG2("init the redis client ~p:~p (~p)", [Host, Port, Index]),
    case gen_tcp:connect(Host, Port, ?TCP_OPTS, Timeout) of
        {ok, Sock} ->
            case do_auth(Sock, Server,Passwd) of
                ok ->
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
    ?DEBUG2("redis client send data:~n~p~n\t=> ~p", [Data, Server]),
    case do_send_recv(Data, Sock, Server) of
        {tcp_error, Reason} ->
            {stop, Reason, {tcp_error, Reason}, State};
        Reply ->
            ?DEBUG2("reply the return ~p", [Reply]),
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

%% convert server info to process registered name
to_regname(Host, Port, Index, First) ->
    L = lists:concat(["redis_client_", Host, "_", Port, "_", Index]),
    case First of
        true ->
            list_to_atom(L);
        false ->
            list_to_existing_atom(L)
    end.

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
do_auth(Sock, Server, Passwd) ->
    ?DEBUG2("auth to server ~p passwd :~p", [Server, Passwd]),
    case Passwd of
        "" ->
            ok;
        _ ->
            do_send_recv([<<"AUTH ">>, Passwd, ?CRLF], Sock, Server)
    end.


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
do_send_reqs([P|Tail], Tag, Req, Monitors) ->
    Monitor = erlang:monitor(process,P),
    catch P ! {'$gen_call', {self(), Tag}, Req},
    do_send_reqs(Tail, Tag, Req, [{P, Monitor} | Monitors]);
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
