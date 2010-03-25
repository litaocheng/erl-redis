-module(redis_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include("ct.hrl").
-include("redis_internal.hrl").
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
    cmd_zset,
    cmd_hash,
    cmd_sort,
    cmd_persistence,
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
    bool(?PF(redis:not_exists_set("key4", "val4"))),
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
    1 = ?PF(redis:list_push_tail("mylist", "e1")),
    {error, _} = ?PF(redis:list_push_tail("key1", "e1")),
    2 = ?PF(redis:list_push_head("mylist", "e2")),
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

cmd_set(_Config) -> 
    true = ?PF(redis:set_add("myset", "s1")),
    false = ?PF(redis:set_rm("myset", "s2")),
    <<"s1">> = ?PF(redis:set_pop("myset")),
    true = ?PF(redis:set_add("myset", "s2")),
    true = ?PF(redis:set_move("myset", "myset2", "s2")),
    0 = ?PF(redis:set_len("myset")),
    1 = ?PF(redis:set_len("myset2")),
    false = ?PF(redis:set_is_member("myset", "s2")),
    true = ?PF(redis:set_is_member("myset2", "s2")),
    ?PF(redis:set_inter(["myset", "myset2"])),
    ?PF(redis:set_inter_store("myset11", ["myset", "myset2"])),
    ?PF(redis:set_union(["myset", "myset2"])),
    ?PF(redis:set_union_store("myset22", ["myset", "myset2"])),
    ?PF(redis:set_diff("myset", ["myset3", "myset2"])),
    ?PF(redis:set_diff_store("myset33", "myset", ["myset3", "myset2"])),

    redis:set_inter_store("myset", ["myset_not_exists"]),
    redis:set_add("myset", "s1"),
    [<<"s1">>] = ?PF(redis:set_members("myset")),
    <<"s1">> = ?PF(redis:set_random_member("myset")),
    
    ok.

cmd_zset(_Config) ->
    true = ?PF(redis:zset_add("myzset", "f1", 1)),
    true = ?PF(redis:zset_rm("myzset", "f1")),
    2 = ?PF(redis:zset_incr("myzset", "f1", 2)),
    1 = ?PF(redis:zset_incr("myzset", "f1", -1)),
    0 = ?PF(redis:zset_index("myzset", "f1")),
    true = redis:zset_add("myzset", "f2", 2),
    true = redis:zset_add("myzset", "f3", 3),
    [<<"f1">>, <<"f2">>, <<"f3">>] = 
        ?PF(redis:zset_range_index("myzset", 0, -1, false)),
    [<<"f2">>, <<"f3">>] = 
        ?PF(redis:zset_range_index("myzset", -2, -1, false)),
    [{<<"f1">>, 1}, {<<"f2">>, 2}, {<<"f3">>, 3}] = 
        ?PF(redis:zset_range_index("myzset", 0, -1, true)),
    ?PF(redis:zset_range_index_reverse("myzset", 0, -1, true)),
    [<<"f1">>, <<"f2">>, <<"f3">>] = 
        ?PF(redis:zset_range_score("myzset", 0, 100, false)),
    [{<<"f1">>, 1}, {<<"f2">>, 2}, {<<"f3">>, 3}] =
        ?PF(redis:zset_range_score("myzset", 0, 100, true)),
    [<<"f1">>] =
        ?PF(redis:zset_range_score("myzset", 0, 100, 0, 1, false)),
    [{<<"f1">>, 1}] = 
        ?PF(redis:zset_range_score("myzset", 0, 100, 0, 1, true)),
    1 = ?PF(redis:zset_rm_by_score("myzset", 0, 1)),
    2 = ?PF(redis:zset_len("myzset")),
    null = ?PF(redis:zset_score("myzset", "f1")),
    ok.

cmd_hash(_Config) -> 
    true = ?PF(redis:hash_set("myhash", "f1", "v1")),
    <<"v1">> = ?PF(redis:hash_get("myhash", "f1")),
    null = ?PF(redis:hash_get("myhash", "f2")),
    true = ?PF(redis:hash_del("myhash", "f1")),
    false = ?PF(redis:hash_del("myhash", "f2")),

    true = ?PF(redis:hash_set("myhash", "f1", "v1")),
    %?PF(redis:hash_exists("myhash", "f1")),
    %?PF(redis:hash_exists("myhash", "f2")),
    true = ?PF(redis:hash_set("myhash", "f2", "v2")),
    2 = ?PF(redis:hash_len("myhash")),
    [<<"f1">>, <<"f2">>] = ?PF(redis:hash_keys("myhash")),
    [<<"v1">>, <<"v2">>] = ?PF(redis:hash_vals("myhash")),
    [{<<"f1">>, <<"v1">>}, {<<"f2">>, <<"v2">>}] = ?PF(redis:hash_all("myhash")),
    ok.

cmd_sort(_Config) ->
    SortOpt = #redis_sort{},
    ?PF(redis:sort("mylist", SortOpt)),

    redis:list_trim("top_uid", 0, -1),
    redis:list_push_tail("top_uid", "2"),
    redis:list_push_tail("top_uid", "5"),
    redis:list_push_head("top_uid", "8"),
    redis:list_push_tail("top_uid", "10"),

    redis:set("age_2", "20"),
    redis:set("age_5", "50"),
    redis:set("age_8", "80"),
    redis:set("age_10", "10"),

    redis:set("lastlogin_1", "2010-03-20"),
    redis:set("lastlogin_2", "2010-02-20"),
    redis:set("lastlogin_5", "2009-08-21"),
    redis:set("lastlogin_8", "2008-05-22"),
    redis:set("lastlogin_10", "2008-04-11"),

    ?PF(redis:sort("top_uid", #redis_sort{})),

    ?PF(redis:sort("top_uid", #redis_sort{
        asc = false, 
        limit = {0, 2}
    })),

    ?PF(redis:sort("top_uid", #redis_sort{
        asc = false,
        alpha = true
    })),

    ?PF(redis:sort("top_uid", #redis_sort{
        by_pat = <<"age_*">>
    })),

    ?PF(redis:sort("top_uid", #redis_sort{
        alpha = true,
        get_pat = [<<"lastlogin_*">>] 
    })),

    ?PF(redis:sort("top_uid", #redis_sort{
        by_pat = <<"age_*">>,
        get_pat = ["#", "age_*", <<"lastlogin_*">>] 
    })),

    ?PF(redis:sort("top_uid", #redis_sort{
        by_pat = <<"lastlogin_*">>,
        get_pat = ["#", "age_*", <<"lastlogin_*">>] 
    })),

    ok.

cmd_persistence(Config) -> 
    ?PF(redis:save()),
    ?PF(redis:bg_save()),
    ?PF(redis:lastsave_time()),
    ?PF(redis:info()),
    ?PF(redis:bg_rewrite_aof()),
    ok.

bool(true) -> ok;
bool(false) -> ok.

non_neg_int(N) when is_integer(N), N >= 0 -> ok.

int(N) when is_integer(N) -> ok.

atom(A) when is_atom(A) -> ok.

now_sec() ->
    {A, B, C} = now(),
    A * 1000000 + B + C div 1000000.
