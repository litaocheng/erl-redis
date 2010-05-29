-module(redis_SUITE).
%% Note: This directive should only be used in test suites.
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include("redis_internal.hrl").
-define(PF(F), 
    fun() ->
        V = F,
        %?INFO2("~n~80..-s\ncall\t:\t~s~nresult\t:\t~p~n~80..=s~n~n", ["-", ??F, V, "="]),
        ct:log(default, "~n~80..-s\ncall\t:\t~s~nresult\t:\t~p~n~80..=s~n~n", ["-", ??F, V, "="]),
        V
    end()).

suite() -> [
    {timetrap,{minutes,2}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    code:add_path("../ebin"),
    {ok, Pid} = redis_client:start(localhost, 6379, ""),
    Redis = redis_client:handler(Pid),
    ok = Redis:flush_all(),
    io:format("Redis is ~p~n", [Redis]),
    [{redis_client, Redis} | Config].

end_per_suite(Config) ->
    Redis = ?config(redis_client, Config),
    redis_client:stop(Redis),
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
        test_generic,
        test_string,
        test_list,
        test_set,
        test_zset,
        % Redis 1.3.4
        test_hash,
        test_sort,
        % Redis 1.3.4
        test_trans,
        test_persistence,
        test_dummy
    ].

%%-------------------------------------------------------------------------
%% Test cases starts here.
%%-------------------------------------------------------------------------
test_dummy(_Config) ->
    ok.

%% test generic commands
test_generic(Config) ->
    Redis = ?config(redis_client, Config),

    bool(?PF(Redis:exists("key1"))),
    bool(?PF(Redis:delete("Key2"))),
    non_neg_int(?PF(Redis:multi_delete(["Key3", "key4", "key5", "key6"]))),
    atom(?PF(Redis:type("key1"))),
    list(?PF(Redis:keys("key*"))),
    ?PF(Redis:random_key()),
    catch ?PF(Redis:rename("key1", "key2")),
    catch ?PF(Redis:rename_not_exists("key1", "key2")),
    ?PF(Redis:dbsize()),
    bool(?PF(Redis:expire("key333", 100000))),
    bool(?PF(Redis:expire_at("key333", 1289138070))),
    int(?PF(Redis:ttl("key333"))),
    catch bool(?PF(Redis:move("key333", 1))),
    ok = ?PF(Redis:select(1)),
    catch ?PF(Redis:move("key333", 0)),
    ok = ?PF(Redis:select(0)),

    % append
    3 = ?PF(Redis:append("key_append", "one")),
    6 = ?PF(Redis:append("key_append", "two")),

    % substr
    null = ?PF(Redis:substr("key_substr", 0, -1)),
    Redis:set("key_substr", "This is a string"),
    <<"This">> = ?PF(Redis:substr("key_substr", 0, 3)),
    <<"ing">> = ?PF(Redis:substr("key_substr", -3, -1)),
    <<"ing">> = ?PF(Redis:substr("key_substr", -3, -1)),
    <<"This is a string">> = ?PF(Redis:substr("key_substr", 0, -1)),
    <<" string">> = ?PF(Redis:substr("key_substr", 9, 100000)),

    ok = ?PF(Redis:flush_db()),
    ok = ?PF(Redis:flush_all()),

    ok.

test_string(Config) -> 
    Redis = ?config(redis_client, Config),

    ok = ?PF(Redis:set("key1", "hello")),
    ok = ?PF(Redis:set("key2", <<"world">>)),
    ok = ?PF(Redis:set("key2-expire", <<"world">>, 1000)),
    <<"hello">> = ?PF(Redis:get("key1")),
    KeyNow = lists:concat(["key", now_sec()]),
    null = ?PF(Redis:get(KeyNow)),
    null = ?PF(Redis:getset(KeyNow, "yes")),
    <<"hello">> = ?PF(Redis:get("key1")),
    <<"hello">> = ?PF(Redis:getset("key1", "hi")),
    <<"world">> = ?PF(Redis:get(<<"key2">>)),
    [<<"hi">>, <<"world">>, null] = ?PF(Redis:multi_get(["key1", <<"key2">>, <<"key_not_exists">>])),
    bool(?PF(Redis:not_exists_set("key4", "val4"))),
    catch ?PF(Redis:multi_set([{"key1", "val1"}, {"key2", "val2"}, {"key22", "val22"}])),
    catch ?PF(Redis:multi_set_not_exists([{"key1", "val1"}, {"key2", "val2"}, {"key22", "val22"}])),
    ok = ?PF(Redis:set("key_num_1", "100")),
    101 = ?PF(Redis:incr("key_num_1")),
    106 = ?PF(Redis:incr("key_num_1", 5)),
    105 = ?PF(Redis:decr("key_num_1")),
    90 = ?PF(Redis:decr("key_num_1", 15)),
    ?PF(Redis:decr("key_num_1", 100)),
    ok.

test_list(Config) -> 
    Redis = ?config(redis_client, Config),
    1 = ?PF(Redis:list_push_tail("mylist", "e1")),
    {error, _} = ?PF(Redis:list_push_tail("key1", "e1")),
    2 = ?PF(Redis:list_push_head("mylist", "e2")),
    int(?PF(Redis:list_len("mylist"))),
    [_|_] = ?PF(Redis:list_range("mylist", 0, -1)),
    [_] = ?PF(Redis:list_range("mylist", 0, 0)),
    ?PF(Redis:list_trim("mylist", 0, 1)),
    ?PF(Redis:list_range("mylist", 0, -1)),
    ?PF(Redis:list_index("mylist", 0)),
    ?PF(Redis:list_set("mylist", 0, "first_v2")),
    ?PF(Redis:list_set("mylist", 2, "sencod_v2")),
    ?PF(Redis:list_rm("mylist", "sencod_v2")),
    ?PF(Redis:list_rm_from_head("mylist", 1, "sencod_v2")),
    ?PF(Redis:list_rm_from_tail("mylist", 1, "sencod_v2")),
    ?PF(Redis:list_pop_head("mylist")),
    ?PF(Redis:list_pop_tail("mylist")),

    ok.

test_set(Config) -> 
    Redis = ?config(redis_client, Config),

    true = ?PF(Redis:set_add("myset", "s1")),
    false = ?PF(Redis:set_rm("myset", "s2")),
    <<"s1">> = ?PF(Redis:set_pop("myset")),
    true = ?PF(Redis:set_add("myset", "s2")),
    1 = ?PF(Redis:set_len("myset")),
    0 = ?PF(Redis:set_len("myset2")),
    true = ?PF(Redis:set_is_member("myset", "s2")),
    false = ?PF(Redis:set_is_member("myset2", "s2")),
    [<<"s2">>] = ?PF(Redis:set_members("myset")),
    true = Redis:set_rm("myset", "s2"),
    true = ?PF(Redis:set_add("myset", "s1")),
    <<"s1">> = ?PF(Redis:set_random_member("myset")),

    catch ?PF(Redis:set_inter(["myset", "myset2"])),
    catch ?PF(Redis:set_inter_store("myset11", ["myset", "myset2"])),
    catch ?PF(Redis:set_union(["myset", "myset2"])),
    catch ?PF(Redis:set_union_store("myset22", ["myset", "myset2"])),
    catch ?PF(Redis:set_diff("myset", ["myset3", "myset2"])),
    catch ?PF(Redis:set_diff_store("myset33", "myset", ["myset3", "myset2"])),
    catch Redis:set_inter_store("myset", ["myset_not_exists"]),
    (catch ?PF(Redis:set_move("myset", "myset2", "s2"))),
    
    ok.

test_zset(Config) ->
    Redis = ?config(redis_client, Config),

    true = ?PF(Redis:zset_add("myzset", "f1", 1)),
    true = ?PF(Redis:zset_rm("myzset", "f1")),
    2 = ?PF(Redis:zset_incr("myzset", "f1", 2)),
    1 = ?PF(Redis:zset_incr("myzset", "f1", -1)),
    % Reids 1.3.4
    %0 = ?PF(Redis:zset_index("myzset", "f1")),
    true = Redis:zset_add("myzset", "f2", 2),
    true = Redis:zset_add("myzset", "f3", 3),
    [<<"f1">>, <<"f2">>, <<"f3">>] = 
        ?PF(Redis:zset_range_index("myzset", 0, -1, false)),
    [<<"f2">>, <<"f3">>] = 
        ?PF(Redis:zset_range_index("myzset", -2, -1, false)),
    [{<<"f1">>, 1}, {<<"f2">>, 2}, {<<"f3">>, 3}] = 
        ?PF(Redis:zset_range_index("myzset", 0, -1, true)),
    ?PF(Redis:zset_range_index_reverse("myzset", 0, -1, true)),
    [<<"f1">>, <<"f2">>, <<"f3">>] = 
        ?PF(Redis:zset_range_score("myzset", 0, 100, false)),
    [{<<"f1">>, 1}, {<<"f2">>, 2}, {<<"f3">>, 3}] =
        ?PF(Redis:zset_range_score("myzset", 0, 100, true)),
    [<<"f1">>] =
        ?PF(Redis:zset_range_score("myzset", 0, 100, 0, 1, false)),
    [{<<"f1">>, 1}] = 
        ?PF(Redis:zset_range_score("myzset", 0, 100, 0, 1, true)),
    1 = ?PF(Redis:zset_rm_by_score("myzset", 0, 1)),
    2 = ?PF(Redis:zset_len("myzset")),
    null = ?PF(Redis:zset_score("myzset", "f1")),
    ok.

test_hash(Config) -> 
    Redis = ?config(redis_client, Config),
    true = ?PF(Redis:hash_set("myhash", "f1", "v1")),
    true = ?PF(Redis:hash_set_not_exists("myhash", "f11", "v11")),
    false = ?PF(Redis:hash_set_not_exists("myhash", "f11", "v11")),

    ok = ?PF(Redis:hash_multi_set("myhash", [{"f1", "v1"}, {"f2", "v2"}])),
    <<"v1">> = ?PF(Redis:hash_get("myhash", "f1")),
    null = ?PF(Redis:hash_get("myhash", "f_not_exist")),
    [<<"v1">>, <<"v2">>] = ?PF(Redis:hash_multi_get("myhash", ["f1", "f2"])),
    [<<"v1">>, <<"v2">>, null] = ?PF(Redis:hash_multi_get("myhash", ["f1", "f2", "f_not_exist"])),

    % hash incr
    {error, _} = ?PF(Redis:hash_incr("myhash", "f1", 5)),
    5 = ?PF(Redis:hash_incr("myhash", "f_num", 5)),

    true = ?PF(Redis:hash_del("myhash", "f1")),
    Redis:hash_del("myhash", "f11"),
    true = ?PF(Redis:hash_del("myhash", "f2")),
    false = ?PF(Redis:hash_del("myhash", "f_not_exist")),
    true = Redis:hash_del("myhash", "f_num"),

    % hash exists
    true = ?PF(Redis:hash_set("myhash", "f1", "v1")),
    false = ?PF(Redis:hash_exists("myhash", "f_not_exist")),
    true = ?PF(Redis:hash_exists("myhash", "f1")),
    
    % hash len
    int(?PF(Redis:hash_len("myhash"))),

    % hash keys/vals/all
    Redis:hash_set(<<"myhash">>, "f2", <<"v2">>),
    [<<"f1">>, <<"f2">>] = ?PF(Redis:hash_keys("myhash")),
    [<<"v1">>, <<"v2">>] = ?PF(Redis:hash_vals("myhash")),
    [{<<"f1">>, <<"v1">>}, {<<"f2">>, <<"v2">>}] = ?PF(Redis:hash_all("myhash")),

    ok.

test_sort(Config) ->
    Redis = ?config(redis_client, Config),
    SortOpt = #redis_sort{},
    ?PF(Redis:sort("mylist", SortOpt)),

    Redis:list_trim("top_uid", 0, -1),
    Redis:list_push_tail("top_uid", "2"),
    Redis:list_push_tail("top_uid", "5"),
    Redis:list_push_head("top_uid", "8"),
    Redis:list_push_tail("top_uid", "10"),

    Redis:set("age_2", "20"),
    Redis:set("age_5", "50"),
    Redis:set("age_8", "80"),
    Redis:set("age_10", "10"),

    Redis:set("lastlogin_1", "2010-03-20"),
    Redis:set("lastlogin_2", "2010-02-20"),
    Redis:set("lastlogin_5", "2009-08-21"),
    Redis:set("lastlogin_8", "2008-05-22"),
    Redis:set("lastlogin_10", "2008-04-11"),

    ?PF(Redis:sort("top_uid", #redis_sort{})),

    ?PF(Redis:sort("top_uid", #redis_sort{
        asc = false, 
        limit = {0, 2}
    })),

    ?PF(Redis:sort("top_uid", #redis_sort{
        asc = false,
        alpha = true
    })),

    ?PF(Redis:sort("top_uid", #redis_sort{
        by_pat = <<"age_*">>
    })),

    ?PF(Redis:sort("top_uid", #redis_sort{
        alpha = true,
        get_pat = [<<"lastlogin_*">>] 
    })),

    ?PF(Redis:sort("top_uid", #redis_sort{
        by_pat = <<"age_*">>,
        get_pat = ["#", "age_*", <<"lastlogin_*">>] 
    })),

    ?PF(Redis:sort("top_uid", #redis_sort{
        by_pat = <<"lastlogin_*">>,
        get_pat = ["#", "age_*", <<"lastlogin_*">>] 
    })),

    ok.

test_trans(Config) ->
    Redis = ?config(redis_client, Config),
    ok = ?PF(Redis:trans_begin()),
    ?PF(Redis:get("k1")),
    ?PF(Redis:hash_get("myhash", "f2")),
    ?PF(Redis:hash_set("myhash", "f2", "v22")),
    ?PF(Redis:trans_commit()),

    ok = ?PF(Redis:trans_begin()),
    ?PF(Redis:get("k1")),
    ?PF(Redis:hash_get("myhash", "f2")),
    ?PF(Redis:hash_set("myhash", "f2", "v22")),
    ok = ?PF(Redis:trans_abort()),
    ok.

test_persistence(Config) -> 
    Redis = ?config(redis_client, Config),
    ?PF(Redis:save()),
    ?PF(Redis:bg_save()),
    ?PF(Redis:lastsave_time()),
    ?PF(Redis:info()),
    ok = ?PF(Redis:slave_off()),
    ok = ?PF(Redis:slave_of("localhost", 16379)),
    ?PF(Redis:bg_rewrite_aof()),
    [{_, _} | _] = ?PF(Redis:config_get("*")),
    ok = ?PF(Redis:config_set("save", [{3600, 100}, {60, 10000}])),
    ok = ?PF(Redis:config_set("maxmemory", "2000000000")),
    ok.

bool(true) -> ok;
bool(false) -> ok.

non_neg_int(N) when is_integer(N), N >= 0 -> ok.

int(N) when is_integer(N) -> ok.
atom(A) when is_atom(A) -> ok.
list(L) when is_list(L) -> ok.

now_sec() ->
    {A, B, C} = now(),
    A * 1000000 + B + C div 1000000.
