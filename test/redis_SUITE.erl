-module(redis_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include("ct.hrl").
-include("redis.hrl").
-define(P(F, D), ?INFO2(F, D)).
    
-define(PFun(F), 
    fun() ->
        V = F,
        ?INFO2("~n~70..=s\ncall\t:\t~s~nresult\t:\t~p~n~70..-s~n", ["=", ??F, V, "-"]),
        V
    end).


suite() -> [
    {timetrap,{minutes,2}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    code:add_path("../ebin"),
    redis_app:start(),
    ok = redis:auth("litao"),
    ok = redis:single_server(localhost, 6379, 1),
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
    cmd_generic,
    cmd_string,
    cmd_list,
    cmd_set,
    cmd_hash,
    test_dummy].

%%-------------------------------------------------------------------------
%% Test cases starts here.
%%-------------------------------------------------------------------------

test_dummy(_Config) -> ok.

%% test generic commands
cmd_generic(Config) ->
    bool(?PFun(redis:exists("key1"))()),
    bool(?PFun(redis:delete("Key2"))()),
    non_neg_int(?PFun(redis:multi_delete(["Key3", "key4"]))()),
    atom(?PFun(redis:type("key1"))()),
    {_, _} = ?PFun(redis:keys("key*"))(),
    ?PFun(redis:random_key())(),
    ?PFun(redis:dbsize())(),
    bool(?PFun(redis:expire("key333", 100000))()),
    bool(?PFun(redis:expire_at("key333", 1289138070))()),
    int(?PFun(redis:ttl("key333"))()),
    bool(?PFun(redis:move("key333", 1))()),
    ok = ?PFun(redis:select(1))(),
    ?PFun(redis:move("key333", 0))(),
    ok = ?PFun(redis:select(0))(),

    ok = ?PFun(redis:flush_db())(),
    ok = ?PFun(redis:flush_all())(),

    ok.

cmd_string(Config) -> 
    ok = ?PFun(redis:set("key1", "hello"))(),
    ok = ?PFun(redis:set("key2", <<"world">>))(),
    <<"hello">> = ?PFun(redis:get("key1"))(),
    KeyNow = lists:concat(["key", now_sec()]),
    null = ?PFun(redis:get(KeyNow))(),
    null = ?PFun(redis:getset(KeyNow, "yes"))(),
    <<"hello">> = ?PFun(redis:get("key1"))(),
    <<"hello">> = ?PFun(redis:getset("key1", "hi"))(),

    ok.

cmd_list(Config) -> ok.
cmd_set(Config) -> ok.
cmd_hash(Config) -> ok.

bool(true) -> ok;
bool(false) -> ok.

non_neg_int(N) when is_integer(N), N >= 0 -> ok.

int(N) when is_integer(N) -> ok.

atom(A) when is_atom(A) -> ok.

now_sec() ->
    {A, B, C} = now(),
    A * 1000000 + B + C div 1000000.
