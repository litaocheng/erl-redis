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

-export([start/0, start_link/2]).
-export([get_sock/1, send/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-record(state, {
        server,             % the host and port
        sock = null,        % the socket
        db = 0,             % the default db index
        dummy
    }).

-define(TCP_OPTS, [inet, binary, {active, false}, 
            {packet, line}, {nodelay, true},
            {send_timeout, 5000}, {send_timeout_close, true}]).

-define(SERVER, ?MODULE).

%% @doc start the redis_sysdata server
-spec start() -> {'ok', any()} | 'ignore' | {'error', any()}.
start() ->
    ?DEBUG2("start ~p ~n", [?SERVER]),
    gen_server:start({local, ?SERVER}, ?MODULE, [], []).

%% @doc start_link the redis_client server
-spec start_link(Server :: single_server(), Timeout :: timeout()) -> 
    {'ok', any()} | 'ignore' | {'error', any()}.
start_link(Server, Timeout) ->
    ?DEBUG2("start_link ~p ~n", [?SERVER]),
    gen_server:start_link({local, ?SERVER}, ?MODULE, {Server, Timeout}, []).

%% @doc return the socket
-spec get_sock(Client :: pid()) -> {'ok', port()}.
get_sock(Client) ->
    {ok, gen_server:call(Client, get_sock)}.

%% @doc send the data
send({Client, Sock} = Conn, Data) when is_port(Sock) ->
    %gen_server:call(Conn, {send, Bin}).
    case gen_tcp:send(Sock, Data) of
        ok -> % receive response
            case gen_tcp:recv(Sock, 0, ?RECV_TIMEOUT) of
                {ok, Packet} ->
                    redis_protocol:parse_response(Conn);
                {error, Reason} ->
                    ?ERROR2("recv message from ~p error:~p", [get_server(Client), Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?ERROR2("send message to ~p error:~p", [get_server(Client), Reason]),
            {error, Reason}
    end.

%%
%% gen_server callbacks
%%
init({Server = {Host, Port}, Timeout}) ->
    case gen_tcp:connect(Host, Port, ?TCP_OPTS, Timeout) of
        {ok, Sock} ->
            % do the auth
            ok = do_auth(Server, Sock),
            % notify the client info to the redis_servers
            ok = set_client(Server, self(), Sock),

            {ok, #state{server = Server, sock = Sock}};
        {error, Reason} ->
            ?ERROR2("connect ~p:~p error", [Host, Port]),
            {stop, Reason}
    end.

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

%% do the auth
do_auth(Server, Sock) ->
    Passwd = redis_servers:passwd(Server),
    ok = send(Sock, [<<"AUTH ">>, Passwd, ?CRLF]).


%% notify the redis_servers the client info
set_client(Server, Pid, Sock) ->
    gen_server:cast(?REDIS_SERVERS, {set_client, Server, Pid, Sock}).

%% get the server info
get_server(Client) ->
    gen_server:call(Client, get_server).

-ifdef(TEST).

-endif.
