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
-define(REDIS_SERVERS, redis_servers).
-define(CONN_SUP, redis_conn_sup).
-define(CONN_TIMEOUT, 1000).
-define(RECV_TIMEOUT, 1000).

%% type defines
-type key() :: binary() | [byte()].
-type index() :: non_neg_integer().
-type pattern() :: [byte()].
-type passwd() :: [byte()] | binary().
-type second() :: non_neg_integer().
-type value_type() :: 'none' | 'string' | 'list' | 'set'.
-type status_code() :: atom().
-type error_reply() :: any().
%-type nil() :: 'nil'.
-type return() :: 'ok' | 'fail'.

-type inet_host() :: atom() | string() | binary().
-type inet_port() :: 0..65535.

-type single_server() :: {inet_host(), inet_port()}.
-type dist_info() :: any().
-type server_type() :: 'undefined' | 'single' | 'dist'.
-type server_info() ::  single_server() | dist_info().
-type connection() :: {pid(), port()}.

-endif. % REDIS_HRL
