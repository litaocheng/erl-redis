%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng@gmail.com
%%% @doc redis internal header file
%%%
%%%----------------------------------------------------------------------
-ifndef(REDIS_INTERNAL_HRL).
-define(REDIS_INTERNAL_HRL, ok).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("redis_log.hrl").
-include("redis.hrl").

%% syntax similar with '?:' in c
-ifndef(IF).
-define(IF(C, T, F), (case (C) of true -> (T); false -> (F) end)).
-endif.

%% some convert macros
-define(B2S(B), binary_to_list(B)).
-define(S2B(S), list_to_binary(S)).

-define(N2S(N), integer_to_list(N)).
-define(S2N(S), list_to_integer(S)).

-define(IOLIST2B(IO), iolist_to_binary(IO)).

%% the separtor
-define(SEP, $\s).
-define(SEP_BIN, <<"\s">>).

%% the CRLF(carr
-define(CRLF, "\r\n").
-define(CRLF_BIN, <<"\r\n">>).

%% for redis module paramters
-define(GROUP_DEFAULT, '$default').
-define(CONTEXT_NORMAL, '$ctx_normal').
-define(CONTEXT_TRANS, '$ctx_trans').
-define(CLIENT_NULL, '$null').

-define(FUN_NULL, '$fnull').
-define(CALL_FUN(V, Fun), 
            case Fun of
                ?FUN_NULL ->
                    V;
                _ ->
                    Fun(V)
            end).

%% the redis supervisor name
-define(REDIS_SUP, redis_sup).
-define(MANAGER_BASE, "redis_manager").
-define(CONN_SUP, redis_conn_sup).
-define(CONN_TIMEOUT, 1000).
-define(RECV_TIMEOUT, 1000).  
-define(CONN_POOL_MIN, 1).
-define(CONN_POOL_MAX, 32).

%% 
%% about types and records
%%
-record(dist_server, {
        data = []       % the data list
    }).

%% type defines
-type key() :: binary() | [byte()].
-type field() :: key().
-type str() :: binary() | [byte()].
-type null() :: 'null'.
-type value() :: null() | str().

-type length() :: non_neg_integer().
-type index() :: integer().
-type pattern() :: binary() | [byte()].
-type passwd() :: [byte()] | binary().
-type second() :: non_neg_integer().
-type timestamp() :: non_neg_integer().
-type value_type() :: 'none' | 'string' | 'list' | 'set' | 'hash'.
-type status_code() :: atom().
-type error() :: {'error', any()}.
-type score() :: integer() | float() | [byte()].

-type inet_host() :: atom() | string() | binary().
-type inet_port() :: 0..65535.
-type inet_server() :: {inet_host(), inet_port()}.

-type group() :: atom().
-type single_server() :: {inet_host(), inet_port(), pos_integer()}.
-type dist_server() :: #dist_server{}.
-type server_info() ::  single_server() | dist_server().
-type server_type() :: 'undefined' | 'single' | 'dist'.
-type mode_info() :: {'single', single_server()} | {'dist', dist_server()}.

-type trans_handler() :: atom().

-endif. % REDIS_INTERNAL_HRL
