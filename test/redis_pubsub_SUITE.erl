-module(redis_pubsub_SUITE).
%% Note: This directive should only be used in test suites.
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include("redis_internal.hrl").
-define(P(F, D), 
    ct:log(default, F, D)).
    
suite() -> [
    {timetrap,{minutes,2}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    code:add_path("../ebin"),
    {ok, PidSub} = redis_client:start(localhost, 6379, ""),
    RedisSub = redis_client:handler(PidSub),
    ok = RedisSub:flush_all(),
    {ok, PidPub} = redis_client:start(localhost, 6379, ""),
    RedisPub = redis_client:handler(PidPub),
    io:format("Redis sub: ~p pub:~p~n", [RedisSub, RedisPub]),
    [{redis_sub, RedisSub}, {redis_pub, RedisPub} | Config].

end_per_suite(Config) ->
    RedisSub = ?config(redis_sub, Config),
    redis_client:stop(RedisSub),
    RedisPub = ?config(redis_pub, Config),
    redis_client:stop(RedisPub),
    crypto:stop(),
    ok.

init_per_testcase(Name, Config) ->
    io:format("..init ~p~n~p~n", [Name, Config]),
    Config.

end_per_testcase(Name, Config) ->
    io:format("...end ~p~n~p~n", [Name, Config]),
    ok.

all() -> 
    [
        test_sub,
        test_pub
    ].

%%-------------------------------------------------------------------------
%% Test cases sthreets here.
%%-------------------------------------------------------------------------

%% test generic commands
test_sub(Config) ->
    RedisSub = ?config(redis_sub, Config),

    RedisSub:subscribe([<<"one">>, <<"two">>], 
        fun callback_sub/2, 
        fun callback_msg/2),

    {error, _} = RedisSub:get("k1"),

    RedisSub:subscribe([<<"three">>],
        fun callback_sub/2,
        fun callback_msg/2),

    RedisSub:unsubscribe([<<"one">>], 
        fun callback_unsub/2),
    RedisSub:unsubscribe(fun callback_sub/2),
    ok.

test_pub(Config) ->
    RedisSub = ?config(redis_sub, Config),
    RedisPub = ?config(redis_pub, Config),

    0 = RedisPub:publish(<<"one">>, "one-msg"),
    RedisSub:subscribe([<<"one">>],
        fun callback_sub/2, 
        fun callback_msg/2),
    1 = RedisPub:publish(<<"one">>, "one-msg"),

    ok.


callback_sub(<<"one">>, 1) ->
    ?P("sub callback with channel one", []),
    ok;
callback_sub(<<"two">>, 2) ->
    ?P("sub callback with channel two", []),
    ok;
callback_sub(<<"three">>, 3) ->
    ?P("sub callback with channel three", []),
    ok.

callback_unsub(<<"one">>, 2) ->
    ?P("unsub callback with channel one", []),
    ok;
callback_unsub(<<"two">>, 1) ->
    ?P("unsub callback with channel two", []),
    ok;
callback_unsub(<<"three">>, 0) ->
    ?P("unsub callback with channel three", []),
    ok.

callback_msg(<<"one">>, <<"one-msg">>) ->
    ?P("msg callback with channel one", []),
    ok;
callback_msg(<<"two">>, <<"two-msg">>) ->
    ?P("msg callback with channel two", []),
    ok;
callback_msg(<<"three">>, <<"three-msg">>) ->
    ?P("msg callback with channel three", []),
    ok.


