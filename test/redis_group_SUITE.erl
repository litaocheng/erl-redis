-module(redis_group_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include("ct.hrl").

suite() -> [
    {timetrap,{minutes,2}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    code:add_path("../ebin"),
    redis_app:start(),
    CacheSevver = {localhost, 16379, 2},
    UserServers = [{localhost, 6379, 2}, {localhost, 6380, 3}],
    ok = redis_app:group_server(cache, single, CacheSevver, "litao"),
    ok = redis_app:group_server(user_info, dist, UserServers, "litao"),
    %ok = Redis:flush_all(),
    Config.

end_per_suite(_Config) ->
    redis_app:stop(),
    crypto:stop(),
    ok.

init_per_testcase(test_cache, Config) ->
    io:format("..init ~p~n~p~n", [test_cache, Config]),
    RedisCache = redis_app:client(cache, single),
    [{redis_client, RedisCache}, {redis_type, single}  | Config];
init_per_testcase(test_user, Config) ->
    io:format("..init ~p~n~p~n", [test_user, Config]),
    RedisUser = redis_app:client(user_info, dist),
    [{redis_client, RedisUser}, {redis_type, dist} | Config].

end_per_testcase(Name, Config) ->
    io:format("...end ~p~n~p~n", [Name, Config]),
    ok.

all() -> 
    [
        test_cache,
        test_user
    ].

%%-------------------------------------------------------------------------
%% Test cases starts here.
%%-------------------------------------------------------------------------

test_cache(Config) ->
    redis_tests:do(Config).

test_user(Config) ->
    redis_tests:do(Config).
