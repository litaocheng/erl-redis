-module(redis_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include("ct.hrl").
-include("redis.hrl").
-define(P(F, D), ?INFO2(F, D)).
    
-define(PFun(F), 
    fun() ->
        V = F,
        ?INFO2("~n~80..-s\ncall\t:\t~s~nresult\t:\t~p~n~80..=s~n~n", ["-", ??F, V, "="]),
        V
    end).


suite() -> [
    {timetrap,{minutes,2}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    code:add_path("../ebin"),
    redis_app:start(),
    %ok = redis:single_server(localhost, 6379, 1, "litao"),
    ok = redis:multi_servers([{localhost, 6379, 2}, {localhost, 6380, 3}], "litao"),
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
    non_neg_int(?PFun(redis:multi_delete(["Key3", "key4", "key5", "key6"]))()),
    atom(?PFun(redis:type("key1"))()),
    {_, _} = ?PFun(redis:keys("key*"))(),
    ?PFun(redis:random_key())(),
    catch ?PFun(redis:rename("key1", "key2"))(),
    catch ?PFun(redis:rename_not_exists("key1", "key2"))(),
    ?PFun(redis:dbsize())(),
    bool(?PFun(redis:expire("key333", 100000))()),
    bool(?PFun(redis:expire_at("key333", 1289138070))()),
    int(?PFun(redis:ttl("key333"))()),
    catch bool(?PFun(redis:move("key333", 1))()),
    ok = ?PFun(redis:select(1))(),
    catch ?PFun(redis:move("key333", 0))(),
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
    <<"world">> = ?PFun(redis:get(<<"key2">>))(),
    [<<"hi">>, <<"world">>, null] = ?PFun(redis:multi_get(["key1", <<"key2">>, <<"key_not_exists">>]))(),
    bool(?PFun(redis:set_not_exists("key4", "val4"))()),
    catch ?PFun(redis:multi_set([{"key1", "val1"}, {"key2", "val2"}, {"key22", "val22"}]))(),
    catch ?PFun(redis:multi_set_not_exists([{"key1", "val1"}, {"key2", "val2"}, {"key22", "val22"}]))(),
    ok = ?PFun(redis:set("key_num_1", "100"))(),
    101 = ?PFun(redis:incr("key_num_1"))(),
    106 = ?PFun(redis:incr("key_num_1", 5))(),
    105 = ?PFun(redis:decr("key_num_1"))(),
    90 = ?PFun(redis:decr("key_num_1", 15))(),
    ?PFun(redis:decr("key_num_1", 100))(),
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
