%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng@gmail.com
%%% @doc redis client header file
%%%
%%%----------------------------------------------------------------------
-ifndef(REDIS_HRL).
-define(REDIS_HRL, ok).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("redis_log.hrl").

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

%% the redis supervisor name
-define(REDIS_SUP, redis_sup).
-define(REDIS_MANAGER_SINGLE, redis_manager_single).
-define(REDIS_MANAGER_DIST, redis_manager_dist).
-define(CONN_SUP, redis_conn_sup).
-define(CONN_TIMEOUT, 1000).
-define(RECV_TIMEOUT, 1000).  
%% 
%% about types and records
%%
-record(dist_server, {
        data = []       % the data list
    }).

%% type defines
-type key() :: binary() | [byte()].
-type str() :: binary() | [byte()].
-type null() :: 'null'.
-type value() :: null() | str().
-type length() :: non_neg_integer().
-type index() :: integer().
-type pattern() :: [byte()].
-type passwd() :: [byte()] | binary().
-type second() :: non_neg_integer().
-type timestamp() :: non_neg_integer().
-type val_type() :: 'none' | 'string' | 'list' | 'set'.
-type status_code() :: atom().
-type error() :: {'error', any()}.

-type inet_host() :: atom() | string() | binary().
-type inet_port() :: 0..65535.

-type server_regname() :: atom().
-type server() :: {inet_host(), inet_port()}.
-type single_server() :: {inet_host(), inet_port(), pos_integer()}.
-type dist_server() :: #dist_server{}.
-type server_info() ::  single_server() | dist_server().
-type server_type() :: 'undefined' | 'single' | 'dist'.
-type mode_info() :: {'single', single_server()} | {'dist', dist_server()}.

-type string_value() :: binary() | [byte()].
-type value_type() :: 'none' | 'string' | 'list' | 'set'.
-type error_reply() :: any().
-type return() :: 'ok' | 'fail'.

-endif. % REDIS_HRL
