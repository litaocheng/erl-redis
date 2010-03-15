%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng <litaocheng@gmail.com>
%%% @doc 
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis_client).
-author('litaocheng@gmail.com').
-vsn('0.1').
-behaviour(gen_server).
-include("redis.hrl").

-export([start/0, start_link/2]).
-export([send/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-record(state, {
        server,             % the host and port
        sock = null,        % the socket
        db = 0,             % the default db index
        dummy
    }).

-define(TCP_OPTS, [inet, binary, {active, once}, 
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

%% @doc send the data
send(Conn, Data) ->
    %gen_server:call(Conn, {send, Bin}).
    gen_tcp:send(Conn, Data).

%%
%% gen_server callbacks
%%
init({Server = {Host, Port}, Timeout}) ->
    case gen_tcp:connect(Host, Port, ?TCP_OPTS, Timeout) of
        {ok, Sock} ->
            % do the auth
            ok = do_auth(Server, Sock),
            {ok, #state{server = Server, sock = Sock}};
        {error, Reason} ->
            ?ERROR2("connect ~p:~p error", [Host, Port]),
            {stop, Reason}
    end.

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

%% do the auth
do_auth(Server, Sock) ->
    Passwd = redis_servers:passwd(Server),
    ok = send(Sock, [<<"AUTH ">>, Passwd, ?CRLF]).

-ifdef(TEST).

-endif.
