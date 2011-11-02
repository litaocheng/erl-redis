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

%% assert(1 =:= 1)
-define(ASSERT(EXP),
    case (EXP) of
        true ->
            ok;
        false ->
            error(??EXP)
    end).


-define(NONE, none).

%% some convert macros
-define(B2S(B), binary_to_list(B)).
-define(S2B(S), list_to_binary(S)).

-define(N2S(N), integer_to_list(N)).
-define(S2N(S), list_to_integer(S)).

-define(IOLIST2B(IO), iolist_to_binary(IO)).

%% the separtor
%-define(SEP, $\s).
-define(SEP, <<"\s">>).

%% the CRLF(carr
%-define(CRLF, "\r\n").
-define(CRLF, <<"\r\n">>).
%-define(CRLF_BIN, <<"\r\n">>).

-define(FUN_NULL, '$fnull').
-define(CALL_FUN(V, Fun), 
            case Fun of
                ?FUN_NULL ->
                    V;
                _ ->
                    Fun(V)
            end).

%% the redis supervisor name
-define(CONN_SUP, redis_conn_sup).
-define(CONN_TIMEOUT, 5000).
-define(COMMAND_TIMEOUT, 2000).  
-define(CONN_POOL_DEF, 5).
-define(CONN_POOL_MIN, 1).
-define(CONN_POOL_MAX, 64).

%% 
%% about types and records
%%

%% type defines
-type key() :: binary() | [byte()].
-type str() :: binary() | [byte()].
-type val() :: null() | str().
-type null() :: 'null'.
-type command() :: binary().
-type uint() :: non_neg_integer().
-type int() :: integer().

-type length() :: non_neg_integer().
-type passwd() :: [byte()] | binary().
-type second() :: non_neg_integer().
-type timestamp() :: non_neg_integer().
-type status_code() :: atom() | binary().
-type error() :: {'error', any()}.
-type value() :: null() | str() | status_code() | list() | error().
-type value_type() :: 'none' | 'string' | 'list' | 'set' | 'zset' | 'hash'.
-type score() :: '-inf' | '+inf' | integer() | float() |
{open, integer() | float()} | {closed, integer() | float()}.
-type aggregate() :: 'sum' | 'min' | 'max'.
-type count() :: non_neg_integer().

-type pattern() :: str().
-type channel() :: str().
-type psubscribe_fun() :: fun((pattern(), count()) -> any()).
-type pmessage_fun() :: fun((pattern(), channel(), str()) -> any()).
-type punsubscribe_fun() :: fun((pattern(), count()) -> any()).
-type subscribe_fun() :: fun((channel(), count()) -> any()).
-type message_fun() :: fun((channel(), str()) -> any()).
-type unsubscribe_fun() :: fun((channel(), count()) -> any()).

-type client() :: pid() | atom().

-type inet_host() :: atom() | string() | binary().
-type inet_port() :: 0..65535.
-type inet_server() :: {inet_host(), inet_port()}.

-type redis_handler() :: {redis, _, _}.

-type trans_handler() :: atom().


-endif. % REDIS_INTERNAL_HRL
