-module(redis_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include("ct.hrl").
-include("redis.hrl").
-define(P(F), 
    begin
        ?INFO2("~n~70..=s\ncall\t:\t~s~nresult\t:\t~p~n~70..=s~n", ["=", ??F, F, "="]),
        %io:format("~n~70..=s\ncall\t:\t~s~nresult\t:\t~p~n~70..=s~n", ["=", ??F, F, "="]),
        F
    end).


suite() -> [
    {timetrap,{minutes,2}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    code:add_path("../ebin"),
    redis_app:start(),
    ok = redis:auth("litao"),
    ok = redis:single_server(localhost, 6379, 2),
    Config.

end_per_suite(_Config) ->
    crypto:stop(),
    ok.

init_per_testcase(Name, Config) ->
    io:format("..init........~p~n~p~n", [Name, Config]),
    Config.

end_per_testcase(Name, Config) ->
    io:format("...end........~p~n~p~n", [Name, Config]),
    ok.

all() -> 
    [
    cmd_common,
    cmd_string,
    cmd_list,
    cmd_set,
    cmd_hash,
    test_dummy].

%%-------------------------------------------------------------------------
%% Test cases starts here.
%%-------------------------------------------------------------------------

test_dummy(_Config) -> ok.

%% test common commands
cmd_common(Config) ->
    bool(?P(redis:exists("key1"))),
    bool(?P(redis:delete("Key2"))),
    non_neg_int(?P(redis:multi_delete(["Key3", "key4"]))),
    atom(?P(redis:type("key1"))),
    {ok, [_|_]} = ?P(redis:keys("key*")),

    ok.

cmd_string(Config) -> ok.
cmd_list(Config) -> ok.
cmd_set(Config) -> ok.
cmd_hash(Config) -> ok.

bool(true) -> ok;
bool(false) -> ok.

non_neg_int(N) when is_integer(N), N >= 0 -> ok.

atom(A) when is_atom(A) -> ok.
