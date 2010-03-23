-module(redis_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include("ct.hrl").
-include("redis.hrl").
-define(P(F, D), ?INFO2(F, D)).
    
-define(PF(F), 
    fun() ->
        V = F,
        ?INFO2("~n~80..-s\ncall\t:\t~s~nresult\t:\t~p~n~80..=s~n~n", ["-", ??F, V, "="]),
        V
    end()).


suite() -> [
    {timetrap,{minutes,2}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    code:add_path("../ebin"),
    redis_app:start(),
    ok = redis:single_server(localhost, 6379, 1, "litao"),
    %ok = redis:multi_servers([{localhost, 6379, 2}, {localhost, 6380, 3}], "litao"),
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
    bool(?PF(redis:exists("key1"))),
    bool(?PF(redis:delete("Key2"))),
    non_neg_int(?PF(redis:multi_delete(["Key3", "key4", "key5", "key6"]))),
    atom(?PF(redis:type("key1"))),
    {_, _} = ?PF(redis:keys("key*")),
    ?PF(redis:random_key()),
    catch ?PF(redis:rename("key1", "key2")),
    catch ?PF(redis:rename_not_exists("key1", "key2")),
    ?PF(redis:dbsize()),
    bool(?PF(redis:expire("key333", 100000))),
    bool(?PF(redis:expire_at("key333", 1289138070))),
    int(?PF(redis:ttl("key333"))),
    catch bool(?PF(redis:move("key333", 1))),
    ok = ?PF(redis:select(1)),
    catch ?PF(redis:move("key333", 0)),
    ok = ?PF(redis:select(0)),

    ok = ?PF(redis:flush_db()),
    ok = ?PF(redis:flush_all()),

    ok.

cmd_string(Config) -> 
    ok = ?PF(redis:set("key1", "hello")),
    ok = ?PF(redis:set("key2", <<"world">>)),
    <<"hello">> = ?PF(redis:get("key1")),
    KeyNow = lists:concat(["key", now_sec()]),
    null = ?PF(redis:get(KeyNow)),
    null = ?PF(redis:getset(KeyNow, "yes")),
    <<"hello">> = ?PF(redis:get("key1")),
    <<"hello">> = ?PF(redis:getset("key1", "hi")),
    <<"world">> = ?PF(redis:get(<<"key2">>)),
    [<<"hi">>, <<"world">>, null] = ?PF(redis:multi_get(["key1", <<"key2">>, <<"key_not_exists">>])),
    bool(?PF(redis:set_not_exists("key4", "val4"))),
    catch ?PF(redis:multi_set([{"key1", "val1"}, {"key2", "val2"}, {"key22", "val22"}])),
    catch ?PF(redis:multi_set_not_exists([{"key1", "val1"}, {"key2", "val2"}, {"key22", "val22"}])),
    ok = ?PF(redis:set("key_num_1", "100")),
    101 = ?PF(redis:incr("key_num_1")),
    106 = ?PF(redis:incr("key_num_1", 5)),
    105 = ?PF(redis:decr("key_num_1")),
    90 = ?PF(redis:decr("key_num_1", 15)),
    ?PF(redis:decr("key_num_1", 100)),
    ok.

cmd_list(Config) -> 
    ok = ?PF(redis:list_push_tail("mylist", "e1")),
    {error, _} = ?PF(redis:list_push_tail("key1", "e1")),
    ok = ?PF(redis:list_push_head("mylist", "e2")),
    int(?PF(redis:list_len("mylist"))),
    [_|_] = ?PF(redis:list_range("mylist", 0, -1)),
    [_] = ?PF(redis:list_range("mylist", 0, 0)),
    ?PF(redis:list_trim("mylist", 0, 1)),
    ?PF(redis:list_range("mylist", 0, -1)),
    ?PF(redis:list_index("mylist", 0)),
    ?PF(redis:list_set("mylist", 0, "first_v2")),
    ?PF(redis:list_set("mylist", 2, "sencod_v2")),
    ?PF(redis:list_rm("mylist", "sencod_v2")),
    ?PF(redis:list_rm_head("mylist", 1, "sencod_v2")),
    ?PF(redis:list_rm_tail("mylist", 1, "sencod_v2")),
    ?PF(redis:list_pop_head("mylist")),
    ?PF(redis:list_pop_tail("mylist")),

    ok.

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
