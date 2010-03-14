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



-endif. % REDIS_HRL
