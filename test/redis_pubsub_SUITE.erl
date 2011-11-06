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
    ok = RedisSub:flushall(),
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
        test_channel,
        test_pattern
    ].

%%-------------------------------------------------------------------------
%% Test cases sthreets here.
%%-------------------------------------------------------------------------

%% test channel pub/sub
test_channel(Config) ->
    RedisSub = ?config(redis_sub, Config),
    RedisPub = ?config(redis_pub, Config),

    ok = RedisSub:subscribe([<<"one">>, <<"two">>], 
        fun cb_channel_sub/2, 
        fun cb_msg/2),

    badmodel = (catch RedisSub:get("k1")),

    ok = RedisSub:subscribe([<<"three">>],
        fun cb_channel_sub/2,
        fun cb_msg/2),

    ok = RedisSub:unsubscribe([<<"one">>], 
        fun cb_channel_unsub/2),
    ok = RedisSub:unsubscribe(fun cb_channel_unsub/2),

    0 = RedisPub:publish(<<"one">>, "no_sub"),
    RedisSub:subscribe([<<"one">>],
        fun cb_channel_sub/2, 
        fun cb_msg/2),
    erlang:put('one-msg', true),
    1 = RedisPub:publish(<<"one">>, "erase"),
    1 = RedisPub:publish(<<"one">>, "get"),
    ok.

%% subscribe callbacks
cb_channel_sub(<<"one">>, 1) ->
    ?P("sub callback with channel one", []),
    ok;
cb_channel_sub(<<"two">>, 2) ->
    ?P("sub callback with channel two", []),
    ok;
cb_channel_sub(<<"three">>, 3) ->
    ?P("sub callback with channel three", []),
    ok.

cb_channel_unsub(<<"one">>, 2) ->
    ?P("unsub callback with channel one", []),
    ok;
cb_channel_unsub(<<"two">>, 1) ->
    ?P("unsub callback with channel two", []),
    ok;
cb_channel_unsub(<<"three">>, 0) ->
    ?P("unsub callback with channel three", []),
    ok.

cb_msg(<<"one">>, <<"erase">>) ->
    erlang:erase('one-msg'),
    ok;
cb_msg(<<"one">>, <<"get">>) ->
    undefined = erlang:get('one-msg'),
    ok;
cb_msg(<<"two">>, <<"two-msg">>) ->
    ?P("msg callback with channel two", []),
    ok;
cb_msg(<<"three">>, <<"three-msg">>) ->
    ?P("msg callback with channel three", []),
    ok.

%%-----------------
%% pattern pub/sub
%%-----------------
test_pattern(Config) ->
    RedisSub = ?config(redis_sub, Config),
    RedisPub = ?config(redis_pub, Config),

    RedisSub:psubscribe("news.*", 
        fun(<<"news.*">>, 1) ->
            "subscribe pattern ok" 
        end,
        fun cb_pmessage1/3),
    RedisSub:psubscribe("news.china.*",
        fun(<<"news.china.*">>, 2) ->
            "subscribe pattern ok"
        end,
        fun cb_pmessage2/3),
    RedisSub:subscribe("news.china.edu",
        fun(<<"news.china.edu">>, 1) ->
            "subscribe channel ok"
        end,
        fun(<<"news.china.edu">>, <<"news 1">>) ->
            ok
        end),
    badmodel = (catch RedisSub:publish("news", "news")),
    3 = RedisPub:publish("news.china.edu", "news 1"),
    2 = RedisPub:publish("news.china.food", "news 2"),
    1 = RedisPub:publish("news.china", "news 3"),
    0 = RedisPub:publish("other_topic", "news 4"),
    ok.

cb_pmessage1(<<"new.*">>, <<"news.china.edu">>, <<"news 1">>) ->
    ok;
cb_pmessage1(<<"new.*">>, <<"news.china.food">>, <<"news 2">>) ->
    ok;
cb_pmessage1(<<"new.*">>, <<"news.china">>, <<"news 3">>) ->
    ok.

cb_pmessage2(<<"new.china.*">>, <<"news.china.edu">>, <<"news 1">>) ->
    ok;
cb_pmessage2(<<"new.china.*">>, <<"news.china.food">>, <<"news 2">>) ->
    ok.
