%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng@gmail.com
%%% @doc the interface for redis
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis).
-author('ltiaocheng@gmail.com').
-vsn('0.1').
-include("redis.hrl").

-export([i/0]).


%% @doc show stats info in the stdout
-spec i() -> 'ok'.
i() ->
    ok.
