-module(redis_single_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

suite() -> [
    {timetrap,{minutes,2}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    code:add_path("../ebin"),
    redis_app:start(),
    ok = redis_app:single_server(localhost, 6379, 1),
    Redis = redis_app:client(single),
    %ok = Redis:flush_all(),
    io:format("Redis is ~p~n", [Redis]),
    [{redis_client, Redis}, {redis_type, single} | Config].

end_per_suite(_Config) ->
    redis_app:stop(),
    crypto:stop(),
    ok.

init_per_testcase(Name, Config) ->
    io:format("..init ~p~n~p~n", [Name, Config]),
    Config.

end_per_testcase(Name, Config) ->
    io:format("...end ~p~n~p~n", [Name, Config]),
    ok.

-include("redis_tests.hrl").

all() -> 
    [
        all_tests()
    ].

%%-------------------------------------------------------------------------
%% Test cases starts here.
%%-------------------------------------------------------------------------
