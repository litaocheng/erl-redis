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
    ok = Redis:flushall(),
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
        test_string,
        test_hash,
        test_list,
        %test_set,
        %test_zset,
        %test_sort,
        %test_trans,
        %test_persistence,
        test_dummy
    ].

%%-------------------------------------------------------------------------
%% Test cases starts here.
%%-------------------------------------------------------------------------
test_dummy(_Config) ->
    ok.

%% test keys commands
test_string(Config) ->
    Redis = ?config(redis_client, Config),

    ?PF(2 = Redis:append("k1", "10")),
    9 = Redis:decr("k1"),
    7 = Redis:decrby("k1", 2),
    <<"7">> = Redis:get("k1"),
    8 = Redis:incr("k1"),
    10 = Redis:incrby("k1", 2),

    ok = Redis:set("k1", "hello world"),
    <<"world">> = Redis:getrange("k1", 6, -1),
    <<"hello world">> = Redis:getrange("k1", 0, -1),
    <<"d">> = Redis:getrange("k1", -1, -1),
    <<"d">> = Redis:getrange("k1", 10, -1),
    11 = Redis:setrange("k1", 6, "home"),
    13 = Redis:setrange("k1", 6, "home!!!"),
    <<"hello home!!!">> = Redis:get("k1"),
    ok = Redis:set("k1", "hello world"),

    <<"hello world">> = Redis:getset("k1", "v1"),
    <<"v1">> = Redis:get(<<"k1">>),
    <<"v1">> = Redis:get(["k", $1]),

    ok = Redis:mset([{"k1", "v1"}, {"k2", "v2"}]),
    [<<"v1">>, <<"v2">>] = Redis:mget(["k1", "k2"]),
    false = Redis:msetnx([{"k1", "v1"}, {"k3", "v3"}]),

    false = Redis:setnx("k1", "v1"),
    ?PF(2 = Redis:strlen("k1")),
    Redis:setex("k11", "v11", 10),

    0 = Redis:setbit("key2", 1, 1),
    1 = Redis:getbit("key2", 1),

    ok = ?PF(Redis:flushdb()),
    ok.

test_hash(Config) -> 
    Redis = ?config(redis_client, Config),

    0 = Redis:hdel("k1", "f1"),
    false = Redis:hexists("k1", "f1"),
    true = Redis:hset("k1", "f1", "v1"),
    true = Redis:hexists("k1", "f1"),
    <<"v1">> = Redis:hget("k1", "f1"),
    1 = Redis:hdel("k1", "f1"),
    0 = Redis:hdel("k1", ["f1", "f2"]),

    1 = Redis:hincrby("k1", "f1", 1),
    -1 = Redis:hincrby("k1", "f1", -2),
    1 = Redis:hincrby("k1", "f1", 2),

    <<"1">> = Redis:hget("k1", <<"f1">>),
    true = Redis:hset("k1", "f2", "v2"),
    [{<<"f1">>, <<"1">>}, {<<"f2">>, <<"v2">>}] = 
        Redis:hgetall("k1"),
    [<<"f1">>, <<"f2">>] = Redis:hkeys("k1"),
    [<<"1">>, <<"v2">>] = Redis:hvals("k1"),
    2 = Redis:hlen("k1"),
    0 = Redis:hlen("knotexists"),

    [<<"1">>, <<"v2">>] = Redis:hmget("k1", [<<"f1">>, "f2"]),
    ok = Redis:hmset("k1", [{"f3", "v3"}, {"f4", "v4"}]),
    [<<"v4">>] = Redis:hmget("k1", ["f4"]),

    true = Redis:hsetnx("k1", "f5", "v5"),
    false = Redis:hsetnx("k1", "f1", "v1"),

    2 = Redis:hdel("k1", ["f1", "f2"]),

    ok = Redis:flushdb(),
    ok.

test_list(Config) -> 
    Redis = ?config(redis_client, Config),
   
    null = Redis:lindex("k1", 1), 
    0 = Redis:linsert("k1", before, "e1", "e2"),
    0 = Redis:llen("k1"),
    [] = Redis:lrange("k1", 0, -1),
    1 = Redis:lpush("k1", "e1"),
    2 = Redis:linsert("k1", before, "e1", "e0"),
    [<<"e0">>, <<"e1">>] = Redis:lrange("k1", 0, -1),
    <<"e0">> = Redis:lpop("k1"),
    null = Redis:lpop("knotexist"),
    2 = Redis:linsert("k1", 'after', "e1", "e2"),
    [<<"e1">>, <<"e2">>] = Redis:lrange("k1", 0, -1),
    [<<"e2">>] = Redis:lrange("k1", -1, -1),
    2 = Redis:llen("k1"),

    3 = Redis:lpush("k1", "e0"),
    4 = Redis:linsert("k1", 'after', "e2", "e1"),
    [<<"e0">>, <<"e1">>, <<"e2">>, <<"e1">>] = Redis:lrange("k1", 0, -1),

    1 = Redis:lrem("k1", 0, "e2"),
    0 = Redis:lrem("k1", 0, "enotexist"),
    1 = Redis:lrem("k1", -1, "e0"),
    [<<"e1">>, <<"e1">>] = Redis:lrange("k1", 0, -1),
    3 = Redis:rpush("k1", "e2"),
    <<"e2">> = Redis:rpop("k1"),
    3 = Redis:rpush("k1", ["e0"]),
    1 = Redis:lrem("k1", 1, "e0"),
    4 = Redis:lpush("k1", ["e-1", "e-2"]),
    ok = Redis:ltrim("k1", 2, -1),

    [<<"e1">>, <<"e1">>] = Redis:lrange("k1", 0, -1),

    2 = Redis:lrem("k1", 2, "e1"),
    [] = Redis:lrange("k1", 0, -1),

    0 = Redis:lpushx("k2", "e2"),
    % k1 not exists
    0 = Redis:lpushx("k1", "e0"),
    0 = Redis:rpushx("k2", "e2"),

    3 = Redis:rpush("k1", ["e0", "e1", "e2"]),
    [<<"e0">>, <<"e1">>, <<"e2">>] = Redis:lrange("k1", 0, -1),
    ok = Redis:lset("k1", 0, "enew0"),
    ?PF({error, _} = Redis:lset("k1", 3, "enew2")),
    
    ok = Redis:ltrim("k1", 0, -1),
    3 = Redis:llen("k1"),
    ok = Redis:ltrim("k1", 0, 1),
    2 = Redis:llen("k1"),

    <<"e1">> = Redis:rpoplpush("k1", "dst"),
    1 = Redis:llen("k1"),
    1 = Redis:llen("dst"),
    2 = Redis:rpushx("dst", "e2"),

    [{<<"k1">>, <<"enew0">>}] = Redis:blpop(["k1", "k2"], 1),
    [{<<"dst">>, <<"e2">>}] = Redis:brpop(["dst"], 1),
    null = Redis:blpop(["knotexist"], 1),

    null = Redis:brpoplpush("knotexist", "k333", 1),
    ok = Redis:ltrim("k1", -1, 0),
    0 = Redis:llen("k1"),
    2 = Redis:push("k1", ["e1", "e2"]),
    [{<<"k1">>, <<"e1">>}] = Redis:brpoplpush("k1", "dst", 1),

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
    queued = Redis:hash_get("myhash", "f2"),
    queued = Redis:hash_set("myhash", "f2", "v22"),
    [<<"v2">>, 0] = ?PF(Redis:trans_commit()),

    ok = Redis:set("k1", "v1"),
    ok = ?PF(Redis:trans_begin()),
    ?PF(Redis:get("k1")),
    queued = Redis:set("k1", "v11"),
    queued = Redis:hash_get("myhash", "f2"),
    queued = Redis:hash_set("myhash", "f2", "v22"),
    ok = ?PF(Redis:trans_abort()),
    <<"v1">> = Redis:get("k1"),

    % include watch
    ok = Redis:watch(["kw1", "kw2"]),
    Redis:trans_begin(),
    Redis:trans_commit(),

    ok = Redis:unwatch(),
    Redis:trans_begin(),
    Redis:trans_abort(),
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
