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
-export([handler/1, pipeline/1]).

-export([get_server/1, get_sock/1]).
-export([set_selected_db/2, get_selected_db/1]).
-export([command/2, multi_command/2]).
-export([psubscribe/4, punsubscribe/3, subscribe/4, unsubscribe/3]).
-export([quit/1, shutdown/1]).

%% some untility functions
-export([msg_send_cb/2]).
-export([name/2, existing_name/2, name/3, existing_name/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).

-compile({inline, [command/2]}).
                
%% pub/sub info(used by channel pub/sub and pattern pub/sub)
-record(pubsub, {
        id,                 % id: {type, pattern() | channel()}
        cb_sub,             % the callback fun for subscribe
        cb_unsub,           % the callback fun for unsubscribe 
        cb_msg              % the callback fun for received message
    }).

-define(PUBSUB_CHANNEL, 'channel').
-define(PUBSUB_PATTERN, 'pattern').

-record(state, {
        server,             % the host and port
        sock = null,        % the socket
        dbindex = 0,        % the selected db index
        ctx = normal,       % the client context
        pubsub_tid          % the pubsub table
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
    call(Client, stop);
stop(Client) ->
    call(Client, stop).

%% @doc get the redis normal handler
-spec handler(client()) -> redis_handler().
handler(Client) ->
    redis:new(Client, false).

%% @doc get the redis pipeline handler
-spec pipeline(client() | redis_handler()) -> redis_handler().
pipeline({redis, Client, _}) ->
    redis:new(Client, true);
pipeline(Client) ->
    redis:new(Client, true).

%% @doc get the server info
-spec get_server(Client :: pid()) -> inet_server().
get_server(Client) ->
    call(Client, get_server).

%% @doc return the socket
-spec get_sock(Client :: pid()) -> {'ok', port()}.
get_sock(Client) ->
    {ok, call(Client, get_sock)}.

%% @doc set the select db
-spec set_selected_db(client(), uint()) -> 
    'ok' | {'error', any()}.
set_selected_db(Client, Index) ->
    call(Client, {set, dbindex, Index}).

%% @doc get the selected db
-spec get_selected_db(atom()) -> uint().
get_selected_db(Client) ->
    call(Client, {get, dbindex}).

%% @doc send the command to redis server
-spec command(client(), iolist()) -> any().
command(Client, Data) ->
    call(Client, {command, {Data, ?COMMAND_TIMEOUT}}).

%% @doc send multiple commands to redis server
-spec multi_command(client(), [iolist()]) -> any().
multi_command(Client, List) ->
    Len = lists:len(List),
    call(Client, {command, {List, Len, ?COMMAND_TIMEOUT}}).

%% @doc subscribe the the patterns, called by redis:psubscribe
psubscribe(Client, Patterns, CbSub, CbMsg) ->
    call(Client, {subscribe, ?PUBSUB_PATTERN, Patterns, CbSub, CbMsg}).

%% @doc unsubscribe from the patterns, called by redis:punsubscribe
punsubscribe(Client, Patterns, Callback) ->
    call(Client, {unsubscribe, ?PUBSUB_PATTERN, Patterns, Callback}).

%% NOTE: the subscribe and unsubscribe functions are asynchronous,
%% it will return 'ok' immediately. you must do the working in 
%% callback functions
subscribe(Client, Channels, CbSub, CbMsg) ->
    %?DEBUG2("subscribe to channels:~p", [Channels]),
    call(Client, {subscribe, ?PUBSUB_CHANNEL, Channels, CbSub, CbMsg}).

unsubscribe(Client, Channels, Callback) ->
    %?DEBUG2("subscribe to channels:~p", [Channels]),
    call(Client, {unsubscribe, ?PUBSUB_CHANNEL, Channels, Callback}).

-spec quit(client()) ->
    'ok' | {'error', any()}.
quit(Client) ->
    call(Client, {shutdown, <<"QUIT">>}).

-spec shutdown(client()) ->
    'ok' | {'error', any()}.
shutdown(Client) ->
    call(Client, {shutdown, <<"SHUTDOWN">>}).

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
                    Tid = do_create_table(),
                    {ok, #state{server = Server, sock = Sock, pubsub_tid = Tid}};
                {tcp_error, Reason} ->
                    {stop, Reason};
                _ ->
                    ?ERROR2("auth failed", []),
                    {stop, auth_failed}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({command, {Data, Timeout}}, _From, 
        State = #state{sock = Sock, server = _Server, ctx = normal}) ->
    ?DEBUG2("redis client command:~n~p~n\t=> ~p", [Data, _Server]),
    Reply = do_send_recv(Data, Sock, Timeout),
    {reply, Reply, State};
handle_call({command, {Data, Len, Timeout}}, _From, 
        State = #state{sock = Sock, server = _Server, ctx = normal}) ->
    ?DEBUG2("redis client multi_command(~p):~p=> ~p", [Len, Data, _Server]),
    Reply = do_send_multi_recv(Data, Len, Sock, Timeout),
    {reply, Reply, State};
handle_call({command, _}, _From, State) ->
    {reply, badmodel, State};

%% abouts pub/sub
handle_call({subscribe, Type, L, CbSub, CbMsg}, _From, 
        State = #state{sock = Sock, pubsub_tid = Tid}) ->
    Cmd = 
    case Type of
        ?PUBSUB_CHANNEL -> <<"subscribe">>;
        ?PUBSUB_PATTERN -> <<"psubscribe">>
    end,
    Data = redis_proto:mbulk_list([Cmd | L]),
    do_send(Sock, Data),
    ok = do_add_pubsub(Type, L, CbSub, CbMsg, Tid),
    {reply, ok, State#state{ctx = pubsub}};
handle_call({unsubscribe, _, _Callback}, _From, State = #state{ctx = normal}) ->
    {reply, {error, in_normal_mode}, State};
handle_call({unsubscribe, Type, L, Callback}, _From, 
        State = #state{sock = Sock, pubsub_tid = Tid}) ->
    Cmd = 
    case Type of
        ?PUBSUB_CHANNEL -> <<"unsubscribe">>;
        ?PUBSUB_PATTERN -> <<"punsubscribe">>
    end,
    Data = redis_proto:mbulk_list([Cmd | L]),
    do_send(Sock, Data),
    ok = do_update_pubsub_unsub(Type, L, Callback, Tid),
    {reply, ok, State};

handle_call({shutdown, Data}, _From, State = #state{sock = Sock}) ->
    case catch do_send_recv(Sock, Data) of
        {error, tcp_closed} ->
            {stop, normal, ok, State};
        _ ->
            {stop, normal, ok, State}
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
    State2 = do_handle_pubsub(Sock, Packet, State),
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

call(Client, Req) ->
    gen_server:call(Client, Req, infinity).

%% create pubsub table
do_create_table() ->
    ets:new(dummy, [ets, private, named_table, {keypos, 1},
            {read_concurrency, true}]).

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
    do_recv(Sock, null, ?COMMAND_TIMEOUT).

do_send_recv(Data, Sock, Timeout) ->
    do_send(Sock, Data),
    do_recv(Sock, null, Timeout).

do_send_multi_recv(Data, Len, Sock, Timeout) ->
    do_send(Sock, Data),
    do_send_multi_recv1(Len, Sock, Timeout, []).
do_send_multi_recv1(0, _Sock, _Timeout, Acc) ->
    lists:reverse(Acc);
do_send_multi_recv1(N, Sock, Timeout, Acc) ->
    Reply = do_recv(Sock, null, Timeout),
    do_send_multi_recv1(N - 1, Sock, Timeout, [Reply | Acc]).

do_send(Sock, Data) ->
    case gen_tcp:send(Sock, Data) of
        ok -> % receive response
            ok;
        {error, Reason} ->
            ?ERROR2("send message error:~p", [Reason]),
            exit({error, Reason})
    end.

do_recv(Sock, PState, Timeout) ->
    receive 
        {tcp, Sock, Packet} ->
            %?DEBUG2("receive packet :~p", [Packet]),
            do_handle_packet(Sock, Packet, PState, Timeout);
        {tcp_closed, _Socket} ->
            ?ERROR2("socket closed by remote peer", []),
            exit({error, tcp_closed});
        {tcp_error, _Socket, Reason} ->
            ?ERROR2("recv message error:~p", [Reason]),
            exit({error, {tcp_error, Reason}})
    after 
        Timeout ->
            ?ERROR2("recv message timeout", []),
            exit({error, recv_timeout})
    end.

do_handle_packet(Sock, Packet, PState, Timeout) ->
    case redis_proto:parse_reply(Packet) of
        {mbulk_more, MB} when PState =:= null -> % multi bulk replies
            %?DEBUG2("need recv mbulk :~p", [MB]),
            recv_bulks(Sock, MB, Timeout);
        {bulk_more, N} -> % bulk reply
            %?DEBUG2("need recv bulk data len:~p", [N]),
            recv_bulk_data(Sock, N, Timeout);
        Val -> % integer, status or error
            %?DEBUG2("parse value is ~p", [Val]),
            inet:setopts(Sock, [{active, once}]),
            Val
    end.

%% recv the bulk data 
recv_bulk_data(Sock, N, Timeout) ->
    ok = inet:setopts(Sock, [{packet, raw}]),
    <<Val:N/bytes, "\r\n">> = recv_n(Sock, N+2, Timeout), % include \r\n
    ok = inet:setopts(Sock, [{packet, line}, {active, once}]),
    Val.

%% recv the multiple bulk replies
recv_bulks(Sock, M, Timeout) ->
    ok = inet:setopts(Sock, [{active, once}]),
    recv_bulks1(Sock, M, [], Timeout).

recv_bulks1(_Sock, 0, Acc, _Timeout) ->
    lists:reverse(Acc);
recv_bulks1(Sock, N, Acc, Timeout) ->
    Bulk = do_recv(Sock, mbulk, Timeout),
    recv_bulks1(Sock, N-1, [Bulk | Acc], Timeout).

recv_n(Sock, N, Timeout) ->
    case gen_tcp:recv(Sock, N, Timeout) of
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

%%------------
%% pub/sub
%%------------

%% add the pubsub entry
do_add_pubsub(Type, L, CbSub, CbMsg, Table) ->
    lists:foreach(
    fun(E) ->
        PubSub = #pubsub{
            id = {Type, E},
            cb_sub = CbSub, 
            cb_msg = CbMsg
        },
        ets:insert(Table, PubSub)
    end, L),
    ok.

%% delete the pubsub entry
do_del_pubsub(Type, Data, Table) ->
    Id = {Type, Data},
    [PubSub] = ets:lookup(Table, Id),
    ets:delete(Table, Id),
    PubSub.

%% update the cb_unsub callback in pubsub
do_update_pubsub_unsub(Type, L, Cb, Table) ->
    lists:foreach(
    fun(E) ->
        Id = {Type, E},
        PubSub =
        case ets:lookup(Table, Id) of
            [] ->
                #pubsub{
                    id = Id,
                    cb_unsub = Cb
                };
            [Exist] ->
                Exist#pubsub{cb_unsub = Cb}
        end,
        ets:insert(Table, PubSub)
    end, L).

%% get the pubsub entry
do_get_pubsub(Type, Data, Table) ->
    [PubSub] = ets:lookup(Table, {Type, Data}),
    PubSub.

%% handle the pubsub tcp data
do_handle_pubsub(Sock, Packet, State = #state{pubsub_tid = Table}) ->
    case do_handle_packet(Sock, Packet, null, ?COMMAND_TIMEOUT) of
        [<<"subscribe">>, Channel, N] ->
            do_handle_subscribe(?PUBSUB_CHANNEL, Channel, N, Table),
            State;
        [<<"psubscribe">>, Pattern, N] ->
            do_handle_subscribe(?PUBSUB_PATTERN, Pattern, N, Table),
            State;
        [<<"unsubscribe">>, Channel, N] ->
            do_handle_unsubscribe(?PUBSUB_CHANNEL, Channel, N, State);
        [<<"punsubscribe">>, Channel, N] ->
            do_handle_unsubscribe(?PUBSUB_PATTERN, Channel, N, State);
        [<<"message">>, Channel, Msg] ->
            #pubsub{cb_unsub = Fun} = do_get_pubsub(?PUBSUB_CHANNEL, Channel, Table),
            catch Fun(Channel, Msg),
            State;
        [<<"pmessage">>, Pattern, Channel, Msg] ->
            #pubsub{cb_unsub = Fun} = do_get_pubsub(?PUBSUB_PATTERN, Pattern, Table),
            catch Fun(Pattern, Channel, Msg),
            State
    end.

%% handle the subscribe 
do_handle_subscribe(Type, Data, N, Table) ->
    #pubsub{cb_sub = Fun} = do_get_pubsub(Type, Data, Table),
    catch Fun(Data, N).

%% handle the unsubscribe
do_handle_unsubscribe(_Type, _Data, 0, #state{pubsub_tid = Table} = State) ->
    ets:delete(Table),
    State#state{ctx = normal};
do_handle_unsubscribe(Type, Data, N, #state{pubsub_tid = Table} = State) ->
    #pubsub{cb_unsub = Fun} = do_del_pubsub(Type, Data, Table),
    catch Fun(Data, N),
    State.

%%---------------
%% Eunit test
%%---------------
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
