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

-export([start/3, start/4, start_link/3, start_link/4]).
-export([stop/1]).
-export([handler/1]).

-export([get_server/1, get_sock/1]).
-export([set_selected_db/2, get_selected_db/1]).
-export([send/2]).
-export([subscribe/4, unsubscribe/2, unsubscribe/3]).
-export([quit/1, shutdown/1]).

%% some untility functions
-export([msg_send_cb/2]).
-export([name/2, existing_name/2, name/3, existing_name/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-compile({inline, [send/2]}).
                
-record(pubsub, {
        cb_sub,
        cb_unsub,
        cb_psub,
        cb_punsub,
        cb_msg
    }).

-record(state, {
        server,             % the host and port
        sock = null,        % the socket
        dbindex = 0,        % the selected db index
        ctx = normal,       % the client context
        pubsub = #pubsub{}  % the pubsub record
    }).

-define(TCP_OPTS, [inet, binary, {active, once}, 
            {packet, line}, 
            {nodelay, false},
            {recbuf, 16#1000},
            {sndbuf, 16#10000},
            {send_timeout, 5000}, {send_timeout_close, true}]).

-spec start(inet_host(), inet_port(), passwd()) ->
    {'ok', any()} | 'ignore' | {'error', any()}.
start(Host, Port, Passwd) ->
    ?DEBUG2("start redis_client ~p:~p", [Host, Port]),
    gen_server:start(?MODULE, {{Host, Port}, Passwd}, []).

-spec start(inet_host(), inet_port(), passwd(), atom()) ->
    {'ok', any()} | 'ignore' | {'error', any()}.
start(Host, Port, Passwd, Name) ->
    ?DEBUG2("start redis_client ~p", [Name]),
    gen_server:start({local, Name}, ?MODULE, {{Host, Port}, Passwd}, []).
    
-spec start_link(inet_host(), inet_port(), passwd()) ->
    {'ok', any()} | 'ignore' | {'error', any()}.
start_link(Host, Port, Passwd) ->
    ?DEBUG2("start_link redis_client ~p:~p", [Host, Port]),
    gen_server:start_link(?MODULE, {{Host, Port}, Passwd}, []).

-spec start_link(inet_host(), inet_port(), passwd(), atom()) ->
    {'ok', any()} | 'ignore' | {'error', any()}.
start_link(Host, Port, Passwd, Name) ->
    ?DEBUG2("start_link redis_client ~p", [Name]),
    gen_server:start_link({local, Name}, ?MODULE, {{Host, Port}, Passwd}, []).

-spec stop(client() | tuple()) -> 'ok'.
stop({redis, Client}) ->
    gen_server:call(Client, stop);
stop(Client) ->
    gen_server:call(Client, stop).

-spec handler(client()) -> tuple().
handler(Client) ->
    redis:new(Client).

%% @doc get the server info
-spec get_server(Client :: pid()) -> inet_server().
get_server(Client) ->
    gen_server:call(Client, get_server).

%% @doc return the socket
-spec get_sock(Client :: pid()) -> {'ok', port()}.
get_sock(Client) ->
    {ok, gen_server:call(Client, get_sock)}.

%% @doc set the select db
-spec set_selected_db(client(), index()) -> 
    'ok' | {'error', any()}.
set_selected_db(Client, Index) ->
    gen_server:call(Client, {set, dbindex, Index}).

%% @doc get the selected db
-spec get_selected_db(atom()) ->
    index().
get_selected_db(Client) ->
    gen_server:call(Client, {get, dbindex}).

%% @doc send the data
-spec send(Client :: pid(), Data :: iolist()) -> any().
send(Client, Data) ->
    gen_server:call(Client, {command, Data}).

%% NOTE: the subscribe and unsubscribe functions are asynchronous,
%% it will return 'ok' immediately. you must do the working in 
%% callback functions
-spec subscribe(client(), [channel()], 
    fun(), fun()) ->
    %fun(channel(), count()) -> any(), 
    %fun([channel(), count()] -> any())) ->
        'ok'.
subscribe(Client, Channels, CbSub, CbMsg) ->
    %?DEBUG2("subscribe to channels:~p", [Channels]),
    Cmd = redis_proto:mbulk_list([<<"subscribe">> | Channels]),
    gen_server:call(Client, {subscribe, Cmd, CbSub, CbMsg}).

-spec unsubscribe(client(), fun()) -> 'ok'.
unsubscribe(Client, Callback) ->
    %?DEBUG2("subscribe to all channels", []),
    Cmd = redis_proto:mbulk(<<"unsubscribe">>),
    gen_server:call(Client, {unsubscribe, Cmd, Callback}).

-spec unsubscribe(client(), [channel()], fun()) -> 'ok'.
unsubscribe(Client, Channels, Callback) ->
    %?DEBUG2("subscribe to channels:~p", [Channels]),
    Cmd = redis_proto:mbulk_list([<<"unsubscribe">> | Channels]),
    gen_server:call(Client, {unsubscribe, Cmd, Callback}).

-spec quit(client()) ->
    'ok' | {'error', any()}.
quit(Client) ->
    gen_server:call(Client, {shutdown, <<"QUIT">>}).

-spec shutdown(client()) ->
    'ok' | {'error', any()}.
shutdown(Client) ->
    gen_server:call(Client, {shutdown, <<"SHUTDOWN">>}).

%% @doc generate one callback functions for pubsub, which send the
%% message to some process
-spec msg_send_cb(pid() | atom(), atom()) ->
    fun().
msg_send_cb(Dest, subscribe) ->
    fun(Channel, N) ->
            catch Dest ! {subscirbe, Channel, N}
    end;
msg_send_cb(Dest, unsubscribe) ->
    fun(Channel, N) ->
            catch Dest ! {unsubscribe, Channel, N}
    end;
msg_send_cb(Dest, message) ->
    fun(Channel, Msg) ->
            catch Dest ! {message, Channel, Msg}
    end.

%% @doc generate process registered name
name(Host, Port) ->
    to_name(Host, Port, true).
existing_name(Host, Port) ->
    to_name(Host, Port, false).

%% @doc generate process registered name
name(Host, Port, UserData) ->
    to_name(Host, Port, UserData, true).
existing_name(Host, Port, UserData) ->
    to_name(Host, Port, UserData, false).

%%
%% gen_server callbacks
%%
init({Server = {Host, Port}, Passwd}) ->
    process_flag(trap_exit, true),
    case gen_tcp:connect(Host, Port, ?TCP_OPTS, ?CONN_TIMEOUT) of
        {ok, Sock} ->
            case do_auth(Sock, Passwd) of
                ok ->
                    {ok, #state{server = Server, sock = Sock}};
                {tcp_error, Reason} ->
                    {stop, Reason};
                _ ->
                    ?ERROR2("auth failed", []),
                    {stop, auth_failed}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({command, Data}, _From, 
        State = #state{sock = Sock, server = _Server, ctx = normal}) ->
    ?DEBUG2("redis client send data:~n~p~n\t=> ~p", [Data, _Server]),
    %?DEBUG2(">>>inet options ~p>>>>", [inet:getopts(Sock, [packet, active])]),
    Reply = (catch do_send_recv(Data, Sock)),
    %ok = inet:setopts(Sock, [{active, once}]),
    %?DEBUG2("message queue len:~p", [process_info(self(), message_queue_len)]),
    %?DEBUG2("<<<inet options ~p<<<<", [inet:getopts(Sock, [packet, active])]),
    ?DEBUG2("reply is: ~p", [Reply]),
    {reply, Reply, State};
handle_call({command, _Data}, _From, State) ->
    {reply, {error, in_pubsub_mode}, State};

handle_call({subscribe, Data, CbSub, CbMsg}, _From, 
        State = #state{sock = Sock, pubsub = PubSub}) ->
    do_send(Sock, Data),
    PubSub2 = PubSub#pubsub{cb_sub = CbSub, cb_msg = CbMsg},
    {reply, ok, State#state{ctx = pubsub, pubsub = PubSub2}};
handle_call({unsubscribe, _, _Callback}, _From, State = #state{ctx = normal}) ->
    {reply, {error, in_normal_mode}, State};
handle_call({unsubscribe, Data, Callback}, _From, 
        State = #state{sock = Sock, pubsub = PubSub}) ->
    do_send(Sock, Data),
    PubSub2 = PubSub#pubsub{cb_unsub = Callback},
    {reply, ok, State#state{pubsub = PubSub2}};
handle_call({shutdown, Data}, _From, State = #state{sock = Sock}) ->
    case catch do_send_recv(Sock, Data) of
        tcp_closed ->
            {stop, normal, ok, State};
        Other ->
            {stop, normal, Other, State}
    end;
handle_call(get_server, _From, State = #state{server = Server}) ->
    {reply, Server, State};
handle_call(get_sock, _From, State = #state{sock = Sock}) ->
    {reply, Sock, State};
handle_call({set, dbindex, Index}, _From, State) ->
    {reply, ok, State#state{dbindex = Index}};
handle_call({get, dbindex}, _From, State = #state{dbindex = Index}) ->
    {reply, Index, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({error, timeout}, State) -> % send timeout
    ?ERROR2("send timeout, the socket closed, process will restart", []),
    {stop, {error, timeout}, State};
handle_info({tcp_closed, Sock}, State = #state{sock = Sock}) ->
    ?ERROR2("socket closed by remote peer", []),
    {stop, tcp_closed, State};
handle_info({tcp, Sock, Packet}, State = #state{sock = Sock, ctx = pubsub}) ->
    State2 = do_pubsub(Sock, Packet, State),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ?DEBUG2("terminate reason:~p", [_Reason]),
    ok.

code_change(_Old, State, _Extra) ->
    {ok, State}.

%%-----------------------------------------------------------------------------
%%
%% internal API
%%
%%-----------------------------------------------------------------------------

to_name(Host, Port, First) ->
    L = lists:concat(["redis_client_", Host, "_", Port]),
    to_atom(L, First).
to_name(Host, Port, UserData, First) when is_list(UserData);
                                is_atom(UserData);
                                is_integer(UserData) ->
    L = lists:concat(["redis_client_", Host, "_", Port, "_", UserData]),
    to_atom(L, First).

to_atom(String, true) ->
    list_to_atom(String);
to_atom(String, false) ->
    list_to_existing_atom(String).

%% do sync send and recv 
do_send_recv(Data, Sock) ->
    do_send(Sock, Data),
    do_recv(Sock, null).

do_send(Sock, Data) ->
    case gen_tcp:send(Sock, Data) of
        ok -> % receive response
            ok;
        {error, Reason} ->
            ?ERROR2("send message error:~p", [Reason]),
            exit({error, Reason})
    end.

do_recv(Sock, PState) ->
    receive 
        {tcp, Sock, Packet} ->
            %?DEBUG2("receive packet :~p", [Packet]),
            do_handle_packet(Sock, Packet, PState);
        {tcp_closed, _Socket} ->
            ?ERROR2("socket closed by remote peer", []),
            throw(tcp_closed);
        {tcp_error, _Socket, Reason} ->
            ?ERROR2("recv message error:~p", [Reason]),
            exit({tcp_error, Reason})
    after 
        ?RECV_TIMEOUT ->
            ?ERROR2("recv message timeout", []),
            throw({recv, timeout})
    end.


do_pubsub(Sock, Packet, State = #state{pubsub = PubSub}) ->
    #pubsub{cb_sub = CbSub, cb_unsub = CbUnSub, cb_msg = CbMsg} = PubSub,
    case do_handle_packet(Sock, Packet, null) of
        [<<"subscribe">>, Channel, N] ->
            do_callback(CbSub, Channel, N),
            State;
        [<<"unsubscribe">>, Channel, 0] ->
            do_callback(CbUnSub, Channel, 0),
            State#state{ctx = normal,
                        pubsub = #pubsub{}};
        [<<"unsubscribe">>, Channel, N] ->
            do_callback(CbUnSub, Channel, N),
            State;
        [<<"message">>, Channel, Msg] ->
            do_callback(CbMsg, Channel, Msg),
            State
    end.

do_callback(undefined, _Arg1, _Arg2) ->
    ok;
do_callback(Fun, Arg1, Arg2) when is_function(Fun, 2) ->
    catch Fun(Arg1, Arg2).

do_handle_packet(Sock, Packet, PState) ->
    case redis_proto:parse_reply(Packet) of
        {mbulk_more, MB} when PState =:= null -> % multi bulk replies
            %?DEBUG2("need recv mbulk :~p", [MB]),
            recv_bulks(Sock, MB);
        {bulk_more, N} -> % bulk reply
            %?DEBUG2("need recv bulk data len:~p", [N]),
            recv_bulk_data(Sock, N);
        Val -> % integer, status or error
            %?DEBUG2("parse value is ~p", [Val]),
            inet:setopts(Sock, [{active, once}]),
            Val
    end.

%% recv the bulk data 
recv_bulk_data(Sock, N) ->
    ok = inet:setopts(Sock, [{packet, raw}]),
    <<Val:N/bytes, "\r\n">> = recv_n(Sock, N+2), % include \r\n
    ok = inet:setopts(Sock, [{packet, line}, {active, once}]),
    Val.

%% recv the multiple bulk replies
recv_bulks(Sock, M) ->
    ok = inet:setopts(Sock, [{active, once}]),
    recv_bulks1(Sock, M, []).

recv_bulks1(_Sock, 0, Acc) ->
    lists:reverse(Acc);
recv_bulks1(Sock, N, Acc) ->
    Bulk = do_recv(Sock, mbulk),
    recv_bulks1(Sock, N-1, [Bulk | Acc]).

recv_n(Sock, N) ->
    case gen_tcp:recv(Sock, N, ?RECV_TIMEOUT) of
        {ok, Data} ->
            Data;
        {error, timeout} ->
            ?ERROR2("recv message timeout", []),
            throw({recv, timeout});
        {error, R} ->
            ?ERROR2("recv message error:~p", [R]),
            exit({tcp_error, R})
    end.
    
%% do the auth
do_auth(Sock, Passwd) ->
    ?DEBUG2("do auth passwd :~p", [Passwd]),
    case Passwd of
        "" ->
            ok;
        _ ->
            do_send_recv([<<"AUTH ">>, Passwd, ?CRLF], Sock)
    end.

-ifdef(TEST).

to_name_test() ->
    ?assertEqual('redis_client_localhost_233', to_name(localhost, 233, true)),
    ?assertEqual('redis_client_localhost_233', to_name(localhost, 233, false)),
    ?assertEqual('redis_client_localhost_233', to_name('localhost', 233, true)),
    ?assertEqual('redis_client_localhost_233', to_name("localhost", 233, true)),

    ?assertEqual('redis_client_localhost_233_1', to_name(localhost, 233, 1, true)),
    ?assertEqual('redis_client_localhost_233_1', to_name(localhost, 233, 1, false)),
    ?assertEqual('redis_client_localhost_233_1', to_name('localhost', 233, "1", true)),
    ?assertEqual('redis_client_localhost_233_1', to_name("localhost", 233, '1', true)),
    ?assertError(function_clause, to_name("localhost", 233, <<1>>, true)),

    ok.

-endif.
