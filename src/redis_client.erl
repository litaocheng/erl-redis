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
-export([get_sock/1, send/2]).

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
send(Client, Data) ->
    ?DEBUG2("send data ~p to server ~p", [Data, get_server(Client)]),
    gen_server:call(Client, {command, Data}).

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

-ifdef(TEST).

-endif.
