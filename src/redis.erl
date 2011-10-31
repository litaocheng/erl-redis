%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng@gmail.com
%%% @doc the interface for redis, all the commands are same with 
%%%     http://redis.io/commands, except for all the function are 
%%%     lowercase of the redis commands
%%%
%%%     this module is parameterized module: 
%%%         Client - the context related client process)
%%%         Pipeline - if in pipeline model
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis, [Client, Pipeline]).
-author('litaocheng@gmail.com').
-vsn('0.1').
-include("redis_internal.hrl").

-export([i/0, version/0, db/0]).

%% keys
-export([del/1, exists/1, expire/2, expireat/2, keys/1, move/2,
        object/2, persist/1, randomkey/0, rename/2, renamenx/2, 
        sort/2, ttl/1, type/1, eval/4]).

%% strings
-export([append/2, decr/1, decrby/2, get/1, getbit/2, getrange/3, getset/2, 
        incr/1, incrby/2, mget/1, mset/2, msetnx/2, set/2,
        setbit/3, setex/3, setnx/2, setrange/3, strlen/1]).

%% hashes
-export([hdel/2, hexists/2, hget/2, hgetall/1, hincrby/3,
        hkeys/1, hlen/1, hmget/2, hmset/2, hset/3, hsetnx/3, hvals/1]).

%% lists
-export([blpop/2, brpop/2, drpoplpush/3, lindex/2, 
        linsert/3, llen/1, lpop/1, lpush/2, lpushx/2,
        lrange/3, lrem/3, lset/3, ltrim/3, rpop/1, rpoplpush/2,
        rpush/2, rpushx/2]).

%% sets
-export([sadd/2, scard/1, sdiff/1, sdiffstore/2, sinter/1,
        sinterstore/2, sismember/2, smemebers/1, smove/3,
        spop/1, srandmember/1, srem/2, sunion/1, sunionstore/2]).

%% sorted sets
-export([zdd/2, zcard/1, zcount/3, zincrby/3, 
        zinterstore/2, zinterstore/3, zinterstore/4,
        zrange/4, zrangebyscore/4, zrank/2, zrem/2,
        zremrangebyrank/3, zremrangebyscore/3, zrevrange/4,
        zrevrangebyscore/4, zrevrank/2, zscore/2, zunionstore/4]).

%% pub/sub
-export([psubscribe/1, subscribe/1, 
        publish/2, 
        punsubscribe/0, punsubscribe/1, 
        unsubscribe/0, unsubscribe/1]).

%% transactions
-export([multi/0, exec/0, discard/0,
        watch/1, unwatch/0]).

%% pipelining
-export([pipelining/1]).

%% connection
-export([auth/1, echo/1, ping/0, quit/0]).

%% server 
-export([config_get/1, config_set/2, config_resetstat/0,
        dbsize/0, debug_object/1, debug_segfault/0,
        flushall/0, flushdb/0, info/0, lastsave/0, slave_of/2, 
        slave_off/0]).

%% other unknown commands
-export([command/1]).

-compile(inline).
-compile({inline_size, 30}).
-import(redis_proto, [mbulk/1, mbulk/2, mbulk/3, mbulk/4, mbulk_list/1]).

-define(MULTI_REPLY(Ret),
    case BadServers of
        [] ->
            Ret;
        [_|_] ->
            {error, Ret, BadServers}
    end).
-define(MULTI_REPLY_OK,
    case BadServers of
        [] ->
            ok;
        [_|_] ->
            {error, BadServers}
    end).

%% @doc show stats info in the stdout
-spec i() -> 'ok'.
i() ->
    ok.

%% @doc return the redis version info
-spec version() -> string().
version() ->
    get_app_vsn().

%% @doc return the currently selected db
-spec db() -> uint().
db() ->
    redis_manager:get_selected_db(Client).

%%-------
%% keys
%%-------

%% @doc delete keys
%% return the number of keys that were removed
-spec del(Keys :: [key()] | key()) -> integer().
del([H|_] = Key) when is_list(H); is_binary(H) ->
    call(mbulk_list([<<"DEL">> | Key]));
del(Key) when is_binary(Key) ->
    call(mbulk(<<"DEL">>, Key)).

%% @doc test if the key exists
-spec exists(key()) -> boolean().
exists(Key) ->
    call(mbulk(<<"EXISTS">>, Key), fun int_may_bool/1),

%% @doc set a timeout on the specified key, after the Time the Key will
%% automatically deleted by server.
-spec expire(Key :: key(), Time :: second()) -> boolean().
expire(Key, Time) ->
    call(mbulk(<<"EXPIRE">>, Key, ?N2S(Time)), fun int_may_bool/1).

%% @doc set a unix timestamp on the specified key, the Key will
%% automatically deleted by server at the TimeStamp in the future.
-spec expire_at(Key :: key(), TimeStamp :: timestamp()) -> boolean().
expire_at(Key, TimeStamp) ->
    call(mbulk(<<"EXPIREAT">>, Key, ?N2S(TimeStamp)), fun int_may_bool/1).

%% @doc return the keys list matching a given pattern
-spec keys(Pattern :: str()) -> [key()].
keys(Pattern) ->
    call(mbulk(<<"KEYS">>, Pattern)), fun may_null_to_list/1).

%% @doc move a key to anthor db
-spec move(key(), uint()) -> boolean().
move(Key, DBIndex) ->
    call(mbulk(<<"MOVE">>, Key, ?N2S(DBIndex))), fun int_may_bool/1).

%% @doc Inspect the internals of Redis objects
%% SubCmd: REFCOUNT, ENCODING, IDLETIME
-spec object(command(), list()) -> integer() | str().
object(SubCmd, Args) ->
    call(mbulk_list([<<"OBJECT">>, SubCmd | Args])).

%% @doc Remove the expiration from a key
-spec persist(key()) -> boolean().
persist(Key) ->
    call(mbulk("PERSIST", Key)), fun int_may_bool/1).

%% @doc return a random key from the keyspace
-spec randomkey() -> key() | null().
randomkey() ->
    call(mbulk(<<"RANDOMKEY">>)).

%% @doc atmoically rename a key
-spec rename(OldKey :: key(), NewKey :: key()) -> 'ok' | error().
rename(OldKey, NewKey) ->
    call(mbulk(<<"RENAME">>, OldKey, NewKey)).

%% @doc  rename a key, only if the new key does not exist
-spec renamenx(Key :: key(), NewKey :: key()) -> boolean().
renmaenx(Key, NewKey) ->
    call(mbulk(<<"RENAMENX">>, Key, NewKey)), fun int_may_bool/1).

%% @doc sort the list in a list, set or sorted set
-spec sort(Key :: key(), SortOpt :: redis_sort()) -> list().
sort(Key, SortOpt) ->
    #redis_sort{
        by_pat = By,
        get_pat = Get,
        limit = Limit,
        asc = Asc,
        alpha = Alpha,
        store = Store
    } = SortOpt,

    ByPart = ?IF(is_empty_str(By), [], [<<"BY">>, By]),
    LimitPart = 
    case Limit of
        {Start, End} ->
            [<<"LIMIT">>, ?N2S(Start), ?N2S(End)];
        _ ->
            []
    end,
    {FieldCount, GetPart} =
    case Get of
        [] ->
            {1, []};
        _ ->
            {length(Get), lists:append([[<<"GET">>, P] || P <- Get])}
    end,
    AscPart = ?IF(is_empty_str(Asc), [], [<<"DESC">>]),
    AlphaPart = ?IF(is_empty_str(Alpha), [], [<<"ALPHA">>]),
    StorePart = ?IF(is_empty_str(Store), [], [<<"STORE">>, Store]),
    call(
        mbulk_list([<<"SORT">>, Key | 
        lists:append([ByPart, LimitPart, GetPart, AscPart, AlphaPart, StorePart])
    ]), fun(R) -> list_to_n_tuple(R, FieldCount) end).

%% @doc get the time to live for a key
-spec ttl(Key :: key()) -> integer().
ttl(Key) ->
    call(mbulk(<<"TTL">>, Key)).

%% @doc determine the type of the value stored at key
-spec type(Key :: key()) -> value_type().
type(Key) ->
    call(mbulk(<<"TYPE">>, Key)).

%% @doc excute a lua script at server side
-spec eval(Script :: str(), NumKeys :: count(), Keys :: [key()], Args :: [str()]) -> value().
eval(Script, NumKeys, Keys, Args) ->
    call(mbulk_list([<<"EVAL">>, Script, ?N2S(NumKeys) | Keys ++ Args])).


%%------------
%% strings
%%------------

%% @doc append a value to the string stored at key,
%%      return the new string length
-spec append(key(), val()) -> integer().
append(Key, Val) ->
    call(mbulk(<<"APPEND">>, Key, Val)).

%% @doc decrease the integer value of key by one
-spec decr(key()) -> integer().
decr(Key) ->
    call(mbulk(<<"DECR">>, Key)).

%% @doc decrease the integer value of key by given number
-spec decr(key(), integer()) -> integer().
decrby(Key, N) ->
    call(mbulk(<<"DECRBY">>, Key, ?N2S(N))).

%% @doc get the value of a key
-spec get(key()) -> val().
get(Key) ->
    call(mbulk(<<"GET">>, Key)).

%% @doc return the bit value at offset in the string value
%% stored at key
-spec getbit(key(), uint()) -> integer().
getbit(Key, Offset) ->
    call(mbulk(<<"GITBIT">>, Key, Offset)).

%% @doc return a substring of the string stored at a key
-spec getrange(key(), int(), int()) -> str().
getrange(Key, Start, End) ->
    call(mbulk(<<"GETRANGE">>, Key, Start, End)).

%% @doc atomaticly set the value and return the old value
-spec getset(key(), str()) -> val().
getset(Key, Val) ->
    call(mbulk(<<"GETSET">>, Key, Val)).

%% @doc increase the integer value of a key by one
-spec incr(key()) -> int().
incr(Key) ->
    call(mbulk(<<"INCR">>, Key)).

%% @doc increase the integer value of key by integer
-spec incrby(key(), int()) -> int().
incrby(Key, N) ->
    call(mbulk(<<"INCRBY">>, Key, ?N2S(N))).

%% @doc get the values of all the given keys
-spec mget([key()]) -> [val()].
mget(Keys) ->
    call(mbulk_list([<<"MGET">> | Keys])).

%% @doc set multiple keys to respective values
-spec mset([{key(), str()}]) -> 'ok'.
mset(KeyVals) ->
    L = [<<"MSET">> | lists:append([[K, V] || {K, V} <- KeyVals])],
    call(mbulk_list(L)).

%% @doc set multiple keys to respective values, only if
%% none of the keys exist
-spec msetnx(KeyVals :: [{key(), str()}]) -> boolean().
msetnx(KeyVals) ->
    L = [<<"MSETNX">> | lists:append([[K, V] || {K, V} <- KeyVals])],
    call(mbulk_list(L)), fun int_may_bool/1).

%% @doc set the string value of the key 
-spec set(key(), str()) -> 'ok'.
set(Key, Val) ->
    call(mbulk(<<"SET">>, Key, Val)).

%% @doc set or clears the bit at offset in the string value
%% stored at key
-spec setbit(key(), uint(), 0 | 1) -> uint().
setbit(Key, Offset, Val) ->
    call(mbulk(<<"SETBIT">>, Key, ?N2S(Offset), Val)).

%% @doc set the string value of the key with expired time
-spec setex(key(), str(), second()) ->
    'ok' | status_code().
setex(Key, Val, Expire) ->
    call(mbulk(<<"SETEX">>, Key, ?N2S(Expire), Val)).

%% @doc set the string value of the key, only if the key 
%% does not exist
-spec setnx(key(), str()) -> boolean().
setnx(Key, Val) ->
    call(mbulk(<<"SETNX">>, Key, Val)).

%% @doc overwrite part of the string at key
%% return the new string length
-spec setrange(key(), uint(), str()) -> uint().
setrange(Key, Start, Val) ->
    call(mbulk(<<"SETRANGE">>, Key, ?N2S(Start), Val)).

%% @doc get the length of the value stored in a key
-spec strlen(key()) -> uint().
strlen(Key) ->
    call(mbulk(<<"STRLEN">>, Key)).

%%------------
%% hashes
%%------------

%% @doc delete one or more hash fields
-spec hdel(key(), [key()]) -> boolean().
hdel(Key, [H|_] = Field) when is_list(H); is_binary(H) ->
    call(mbulk_list([<<"HDEL">> | Field]));
hdel(Key, Field) ->
    call(mbulk(<<"HDEL">>, Field)).

%% @doc determine if a hash field exists
-spec hexists(key(), key()) -> boolean().
hexists(Key, Field) ->
    call(mbulk(<<"HEXISTS">>, Key, Field)), fun int_may_bool/1).

%% @doc get the value of the a hash field
-spec hget(key(), key()) -> val().
hget(Key, Field) ->
    call(mbulk(<<"HGET">>, Key, Field)).

%% @doc get all the fields and values in a hash
-spec hgetall(key()) -> [{key(), str()}].
hgetall(Key) ->
    call(mbulk(<<"HGETALL">>, Key)), fun list_to_kv_tuple/1).

%% @doc increment the integer value of a hash filed by
%% given numer
-spec hincrby(key(), key(), int()) -> int().
hincrby(Key, Field, N) ->
    call(mbulk(<<"HINCRBY">>, Key, Field, ?N2S(N))).

%% @doc get all the fields in hash
-spec hkeys(key()) -> [val()].
hkeys(Key) ->
    call(mbulk(<<"HKEYS">>, Key)).

%% @doc get the number of fields in hash
-spec hlen(key()) -> integer().
hlen(Key) ->
    call(mbulk(<<"HLEN">>, Key)).

%% @doc get the values of all specified fields
-spec hmget(key(), [key()]) -> [val()].
hmget(Key, Fields) ->
    call(mbulk_list([<<"HMGET">>, Key | Fields])).

%% @doc Set multiple hash fields to multiple values
-spec hmset(key(), [{key(), str()}]) -> 'ok'.
hmset(Key, FieldVals) ->
    L = [<<"HMSET">>, Key | lists:append([[F, V] || {F, V} <- FieldVals])],
    call(mbulk_list(L)).

%% @doc set the string value of a hash filed with 
-spec hset(key(), key(), str()) -> boolean().
hset(Key, Field, Val) ->
    call(mbulk(<<"HSET">>, Key, Field, Val)), fun int_my_bool/1).

%% @doc Set the value of a hash field, only if the field 
%% does not exist
-spec hsetnx(key(), key(), str()) -> boolean().
hsetnx(Key, Field, Val) ->
    call(mbulk(<<"HSETNX">>, Key, Field, Val)), fun int_may_bool/1).

%% @doc return all the values in hash
-spec hvals(key()) -> [val()].
hvals(Key) ->
    call(mbulk(<<"HVALS">>, Key)).

%%------------------------
%% lists
%%------------------------

%% @doc Remove and get the first element in a list, or block
%% until one is available
-spec blpop([key()], timeout()) -> val().
blpop(Keys, Timeout) ->
    L = mbulk_list([<<"BLPOP">> | Keys ++ [timeout_val(Timeout)]]),
    call(L).

%% @doc Remove and get the last element in a list, 
%% or block until one is available
-spec brpop([key()], timeout()) ->
    value().
brpop(Keys, Timeout) ->
    call(mbulk_list([<<"BRPOP">> | (Keys ++ [timeout_val(Timeout)])]), Timeout).

%% @doc Pop a value from a list, push it to another list 
%% and return it; or block until one is available
-spec brpoplpush(key(), key(), timeout()) -> val().
brpoplpush(Src, Dst, Timeout) ->
    call(mbulk(<<"BRPOPLPUSH">>, Src, Dst, timeout_str(Timeout))). 

%% @doc Get an element from a list by its index
-spec lindex(key(), uint()) -> val().
lindex(Key, Idx) ->
    call(mbulk(<<"LINDEX">>, Key, ?N2S(Idx))).

%% @doc Insert an element before or after another 
%% element in a list
-spec linsert(key(), 'before' | 'after', key(), val()) -> int().
linsert(Key, Pos, Pivot, Val) when Pos =:= 'before'; Pos =:= 'after' ->
    PosStr = 
    case Pos of 
        before -> <<"BEFORE">>;
        _ -> <<"AFTER">>
    end,
    call(mbulk(<<"LINSERT">>, PosStr, Pivot, Val)).

%% @doc get the length of the list
-spec llen(key()) -> uint().
llen(Key) ->
    call(mbulk(<<"LLEN">>, Key)).

%% @doc remove and get the first element of the list
-spec lpop(key()) -> val().
lpop(Key) ->
    call(mbulk(<<"LPOP">>, Key)).

%% @doc add multiple values to the head of the list 
-spec lpush(key(), str() | [str()]) -> uint().
lpush(Key, [H|_] = Val) when is_list(H); is_binary(H) ->
    call(mbulk_list([<<"LPUSH">>, Key | Val]));
lpush(Key, Val) ->
    call(mbulk(<<"LPUSH">>, Key, Val)).

%% @doc add value to the head of the list, only if the list exist
-spec lpushx(key(), str()) -> uint().
lpushx(Key, Val) ->
    call(mbulk(<<"LPUSHX">>, Key, Val)).

%% @doc Get a range of elements from a list
-spec lrange(key(), int(), int()) -> [val()].
list_range(Key, Start, End) ->
    call(mbulk(<<"LRANGE">>, Key, ?N2S(Start), ?N2S(End))), fun may_null_to_list/1).

%% @doc Remove elements from a list
%% N > 0, from head to tail
%% N < 0, from tail to head
%% N = 0, remove all elements equal to Val
-spec lrem(key(), int(), str()) -> uint().
lrem(Key, N, Val) ->
    call(mbulk(<<"LREM">>, Key, ?N2S(N), Val)).

%% @doc Set the value of an element in a list by its index
-spec lset(key(), int(), str()) -> 'ok'.
lset(Key, Idx, Val) ->
    call(mbulk(<<"LSET">>, Key, ?N2S(Idx), Val)).

%% @doc Trim a list to the specified range
-spec ltrim(key(), int(), int()) -> 'ok'.
ltrim(Key, Start, End) ->
    call(mbulk(<<"LTRIM">>, Key, ?N2S(Start), ?N2S(End))).

%% @doc Remove and get the last element in a list
-spec rpop(key()) -> val().
rpop(Key) ->
    call(mbulk(<<"RPOP">>, Key)).

%% @doc Remove the last element in a list, append it to 
%% another list and return it
-spec rpoplpush(key(), key()) -> val().
rpoplpush(Src, Dst) ->
    call(mbulk(<<"RPOPLPUSH">>, Src, Dst)).

%% @doc Append one or multiple values to a list
-spec rpush(key(), str() | [str()]) -> uint().
rpush(Key, [H|_] = Val) when is_list(H); is_binary(H) ->
    call(mbulk_list([<<"RPUSH">>, Key | Val]));
rpush(Key, Val) ->
    call(mbulk(<<"RPUSH">>, Key, Val)).

%% @doc add value to the tail of the list, only if the list exist
-spec rpushx(key(), str()) -> uint().
rpushx(Key, Val) ->
    call(mbulk(<<"RPUSHX">>, Key, Val)).

%%--------------------------------------
%% sets (in set all members is distinct)
%%--------------------------------------

%% @doc Add one or more members to a set
-spec sadd(key(), str()) -> uint().
sadd(Key, [H|_] = Mem) when is_list(H); is_binary(H) ->
    call(mbulk_list([<<"SADD">>, Key | Mem]));
sadd(Key, Mem) ->
    call(mbulk(<<"SADD">>, Key, Mem)).

%% @doc return the number of elements in set
-spec scard(key()) -> uint().
scard(Key) ->
    call(mbulk(<<"SCARD">>, Key)).

%% @doc Subtract multiple sets
-spec sdiff(key(), [key()]) -> [val()].
sdiff(First, Keys) ->
    call(mbulk_list([<<"SDIFF">>, First | Keys])).

%% @doc Subtract multiple sets and store the resulting 
%% set in a key
-spec sdiffstore(key(), key(), [key()]) -> uint().
sdiffstore(Dst, First, Keys) ->
    call(mbulk_list([<<"SDIFFSTORE">>, Dst, First | Keys])).  

%% @doc return the intersection between the sets
-spec sinter([key()]) -> [val()].
sinter(Keys) ->
    call(mbulk_list([<<"SINTER">> | Keys])).

%% @doc Intersect multiple sets and store the 
%% resulting set in a key
-spec sinterstore(key(), [key()]) -> uint().
sinterstore(Dst, Keys) ->
    call(mbulk_list([<<"SINTERSTORE">>, Dst | Keys])).

%% @doc Determine if a given value is a member of a set
-spec sismember(key(), str()) -> boolean().
sismember(Key, Mem) ->
    call(mbulk(<<"SISMEMBER">>, Key, Mem)), fun int_may_bool/1).

%% @doc get all the members in the set
-spec smembers(key()) -> boolean().
smembers(Key) ->
    call(mbulk(<<"SMEMBERS">>, Key)).

%% @doc Move a member from one set to another
-spec smove(key(), key(), str()) -> boolean().
smove(Src, Dst, Mem) ->
    call(mbulk(<<"SMOVE">>, Src, Dst, Mem)), fun int_may_bool/1).

%% @doc Remove and return a random member from a set
-spec spop(key()) -> val().
spop(Key) ->
    call(mbulk(<<"SPOP">>, Key)).

%% @doc Get a random member from a set
-spec srandmember(key()) -> val().
srandmember(Key) ->
    call(mbulk(<<"SRANDMEMBER">>, Key)).

%% @doc Remove one or more members from a set
-spec srm(key(), key() | [key()]) -> boolean().
srm(Key, [H|_] = Mem) when is_list(H); is_binary(H) ->
    mbulk_list([<<"SREM">>, Key | Mem]);
srm(Key, Mem) ->
    call(mbulk(<<"SREM">>, Key, Mem)).

%% @doc  Add multiple sets
-spec sunion([key()]) -> [val()].
sunion(Keys) ->
    call(mbulk_list([<<"SUNION">> | Keys])).

%% @doc Add multiple sets and store the resulting set in a key
-spec sunionstore(key(), [key()]) -> uint().
sunionstore(Dst, Keys) ->
    call(mbulk_list([<<"SUNIONSTORE">>, Dst | Keys])).

%%--------------------
%% sorted sets
%%--------------------

%% @doc Add one or more members to a sorted set, or 
%% update its score if it already exists
-spec zadd(key(), [{score(), str()}]) -> uint().
zadd(Key, Mem) ->
    L = [[?N2S(Score), Val] || {Score, Val} <- Mem],
    call(mbulk_list([<<"ZADD">>, Key | L])).

%% @doc return the number of elements in sorted set
-spec zcard(key()) -> uint().
zcard(Key) ->
    call(mbulk(<<"ZCARD">>, Key)).

%% @doc Count the members in a sorted set with 
%% scores within the given values
-spec zcount(key(), score(), score()) -> uint().
zcount(Key, Min, Max) ->
    MinStr = score_to_str(Min),
    MaxStr = score_to_str(Max),
    call(mbulk(<<"ZCOUNT">>, Key, MinStr, MaxStr))).

%% @doc Increment the score of a member in a sorted set
-spec zincrby(key(), int(), key()) -> score().
zincrby(Key, N, Mem) ->
    call(mbulk(<<"ZINCRBY">>, Key, ?N2S(N), Mem)), fun str_to_score/1).

%% @doc Intersect multiple sorted sets and store 
%% the resulting sorted set in a new key
zinterstore(Dst, Keys) ->
    zinterstore(Dst, Keys, [], sum);
zinterstore(Dst, Keys, Weights) ->
    ?ASSERT(length(Keys) =:= length(Weights)),
    zinterstore(Dst, Keys, Weights, sum);
zinterstore(Dst, Keys, Weights, Aggregate) ->
    do_zset_store(<<"ZINTERSTORE">>, Dst, Keys, Weights, Aggregate).

%% @doc Return a range of members in a sorted set, by index
-spec zrange(key(), int(), int(), boolean()) -> 
    [val()] | [{key(), val()}].
zrange(Key, Start, End, WithScore) ->
    do_zrange(<<"ZRANGE">>, Key, Start, End, WithScore).

%% @doc Return a range of members in a sorted set, by score
-spec zrangebyscore(key(), score(), score(), boolean()) -> 
    [val()] | [{key(), val()}].
zrangebyscore(Key, Min, Max, WithScore) ->
    do_zrange_by_score(<<"ZRANGEBYSCORE">>, Key, Start, End, WithScore).

%% @doc Determine the index of a member in a sorted set
-spec zrank(key(), key()) -> int() | null().
zrank(Key, Mem) ->
    call(mbulk(<<"ZRANK">>, Key, Mem)).

%% @doc Remove one or more members from a sorted set
-spec zrem(key(), key() | [key()]) -> uint().
zrem(Key, [H|_] = Mem) when is_list(H); is_binary(H) ->
    call(mbulk_list(<<"ZREM">>, Key | Mem));
zrem(Key, Mem) ->
    call(mbulk(<<"ZREM">>, Key, Mem)).

%% @doc Remove all members in a sorted set within the given indexes
-spec zremrangebyrank(key(), uint(), uint()) -> uint().
zremrangebyrank(Key, Start, End) ->
    call(mbulk(<<"ZREMRANGEBYRANK">>, Key, ?N2S(Start), ?N2S(End))).

%% @doc Remove all members in a sorted set within the given scores
-spec zremrangebyscore(key(), score(), score()) -> uint().
zremrangebyscore(Key, Min, Max) ->
    call(mbulk(<<"ZREMRANGEBYSCORE">>, Key, 
            score_to_str(Min), score_to_str(Max))).

%% @doc Return a range of members in a sorted set, by index,
%% with scores ordered from high to low
-spec zrevrange(key(), int(), int(), boolean()) -> 
    [value()] | [{key(), value()}].
zrevrange(Key, Start, End, WithScore) ->
    do_zrange(<<"ZREVRANGE">>, Key, Start, End, WithScore).

%% @doc Return a range of members in a sorted set, by score,
%% with scores ordered from high to low
-spec zrevrangebyscore(key(), score(), score(), boolean()) -> 
    [val()] | [{key(), val()}].
zrevrangebyscore(Key, Min, Max, WithScore) ->
    do_zrange_by_score(<<"ZREVRANGEBYSCORE">>, Key, Start, End, WithScore).

%% @doc Determine the index of a member in a sorted set, 
%% with scores ordered from high to low
-spec zrevrange(key(), key()) -> int() | null().
zrevrange(Key, Mem) ->
    call(mbulk(<<"ZREVRANK">>, Key, Mem)).

%% @doc Get the score associated with the given member 
%% in a sorted set
-spec zscore(key(), key()) -> score() | null().
zscore(Key, Mem) ->
    call(mbulk(<<"ZSCORE">>, Key, Mem)).

%% @doc Add multiple sorted sets and store 
%% the resulting sorted set in a new key
-spec zunionstore(key(), [key()]) ->
    integer().
zunionstore(Dst, Keys) ->
    zunionstore(Dst, Keys, [], sum).
zunionstore(Dst, Keys, Weights, Aggregate) ->
    do_zset_store(<<"ZUNIONSTORE">>, Dst, Keys, Weights, Aggregate).

%%----------------
%% pub/sub 
%%----------------

%% @doc Listen for messages published to channels 
%% matching the given patterns
%% CbSub: called when received subscribe reply
%% CbMsg: called when received msg
%% (can't pipeline)
-spec psubscribe(pattern() | [pattern()], 
    psubscribe_fun(), pmessage_fun()) -> 'ok'.
psubscribe(Pattern, CbSub, CbMsg) 
    when is_function(CbSub, 2),
    is_function(CbMsg, 3) ->
    L = may_single_to_list(Pattern),
    redis_client:psubscribe(Client, L, CbSub, CbMsg).

%% @doc Listen for messages published to the given channels
%% (can't pipeline)
-spec subscribe(channel() | [channel()], 
    subscribe_fun(), message_fun()) -> 'ok'.
subscribe(Channel, CbSub, CbMsg)
    when is_function(CbSub, 2),
    is_function(CbMsg, 2) ->
    L = may_single_to_list(Channel),
    redis_client:subscribe(Client, L, CbSub, CbMsg).

%% @doc publish a message to channel
-spec publish(channel(), str()) -> uint().
publish(Channel, Msg) ->
    call(mbulk(<<"PUBLISH">>, Channel, Msg)).

%% @doc unsubscribe to all channels
%% CbUnSub: called when received unsubscribe reply
%% (can't pipeline)
-spec punsubscribe(punsubscribe_fun()) -> 'ok'.
punsubscribe(CbUnSub) ->
    punsubscribe([], CbUnSub).
-spec punsubscribe(str() | [str()], punsubscribe_fun()) -> 'ok'.
punsubscribe(Pattern, CbUnSub) ->
    when is_function(CbUnSub, 2) ->
    L = may_single_to_list(Pattern),
    redis_client:punsubscribe(Client, L, CbUnSub).

%% @doc unsubscribe to all channels
%% (can't pipeline)
-spec unsubscribe(unsubscribe_fun()) -> 'ok'.
unsubscribe(CbUnSub) ->
    unsubscribe([], CbUnSub).
-spec unsubscribe(channel() | [channel()], unsubscribe_fun()) -> 'ok'.
unsubscribe(Channel, CbUnSub)
    when is_function(CbUnSub, 2) ->
    L = may_single_to_list(Pattern),
    redis_client:unsubscribe(Client, L, CbUnSub).

%%---------------------------
%% transaction commands 
%%---------------------------

%% @doc Mark the start of a transaction block
-spec multi() -> 'ok'.
multi() ->
    call(mbulk(<<"MULTI">>)).

%% @doc Execute all commands issued after MULTI
-spec exec() -> any().
exec() ->
    call(mbulk(<<"EXEC">>)).

%% @doc Discard all commands issued after MULTI
-spec discard() -> 'ok'.
discard() ->
    call(mbulk(<<"DISCARD">>)).

%% @doc watch some keys, before the EXEC command in transaction, if the watched keys
%% were changed, the transaction failed, otherwise the transaction success
-spec watch(key() | [key()]) -> 'ok'.
watch(Key) ->
    L = may_single_to_list(Key)
    call(mbulk_list([<<"WATCH">> | L])).

%% @doc Forget about all watched keys
-spec unwatch() -> 'ok'.
unwatch() ->
    call(mbulk(<<"UNWATCH">>)).

%%------------
%% pipeline
%%------------

%% @doc pipeline commands
%% e.g.
%% F = 
%% fun() ->
%%  exists("key1"),
%%  set("key1", "val1"),
%%  get("key1")
%% end,
%% pipeline(F).
pipeline(Fun) when is_list(Fun, 0) ->
    case Pipeline of
        true ->
            Fun(),
            Cmds = get_pipeline_cmd(),
            FunList = get_pipeline_fun(),
            ResultList = redis_client:commands(Cmds),
            lists:mapfoldl(
            fun(R, [F|T]) ->
                case F of
                    ?NONE ->
                        {R, T};
                    _ ->
                        {F(R), T}
                end
            end, FunList, ResultList);
        false ->
            throw(in_normal_model)
    end.

%%----------------------
%% connections
%%----------------------

%% @doc simple password authentication
-spec auth(iolist()) -> 'ok' | error().
auth(Pass) ->
    call(mbulk(<<"AUTH">>, Pass)).

%% @doc Echo the given string
-spec echo(str()) -> str().
echo(Msg) ->
    call(mbulk(<<"ECHO">>, Msg)).

%% @doc ping the server
-spec ping() -> status_code().
ping() ->
    call(mbulk(<<"PING">>)).

%% @doc close the connection
%% (can't pipeline)
-spec quit() -> 'ok'.
quit() ->
    redis_client:quit(Client).

%% @doc Change the selected database for the current connection
%% (can't pipeline)
-spec select(uint()) -> status_code().
select(Index) ->
    R = call(mbulk(<<"SELECT">>, ?N2S(Index))),
    case R of
        ok ->
            redis_client:set_selected_db(Client, Index);
        Other ->
            Other
    end.

%%---------
%% server
%%---------

%% @doc Asynchronously rewrite the append-only file
-spec bgrewriteaof() -> 'ok'.
bgrewriteaof() ->
    call(mbulk(<<"BGREWRITEAOF">>)).

%% @doc Asynchronously save the dataset to disk
-spec bgsave() -> status_code().
bgsave() ->
    call(mbulk(<<"BGSAVE">>)).

%% @doc Get the value of a configuration parameter
-spec config_get(pattern()) -> [{str(), str()}].
config_get(Pattern) ->
    call(mbulk(<<"CONFIG">>, <<"GET">>, Pattern), fun list_to_kv_tuple/1).

%% @doc Set a configuration parameter to the given value
-spec config_set(str(), str()) -> status_code().
config_set(Par, L) when Par =:= "save"; Par =:= <<"save">> ->
    Str = to_save_str(L),
    call(mbulk(<<"CONFIG">>, <<"SET">>, <<"save">>, Str));
config_set(Par, Val) when is_list(Par) ->
    ValStr =
    if 
        is_integer(Val) ->
            ?N2S(Val);
        is_list(Val) ->
            Val
    end,
    call(mbulk(<<"CONFIG">>, <<"SET">>, Par, ValStr)).

%% @doc Reset the stats returned by INFO
-spec config_resetstat() -> 'ok'.
config_resetstat() ->
    call(mbulk(<<"CONFIG">>, <<"RESETSTAT">>)).

%% @doc Return the number of keys in the selected database
-spec dbsize() -> count().
dbsize() ->
    call(mbulk(<<"DBSIZE">>)).

%% @doc Get debugging information about a key
-spec debug_object(key()) -> val().
debug_object(Key) ->
    call(mbulk(<<"DEBUG">>, <<"OBJECT">>, Key)).

%% @doc Make the server crash
-spec debug_segfault() -> no_return().
debug_segfault() ->
    call(mbulk(<<"DEBUG">>, <<"SEGFAULT">>)).

%% @doc Remove all keys from all databases
-spec flushall() -> status_code().
flushall() ->
    call(mbulk(<<"FLUSHALL">>)).

%% @doc Remove all keys from the current database
-spec flushdb() -> status_code().
flushdb() ->
    call(mbulk(<<"FLUSHDB">>)).

%% @doc return the info about the server
-spec info() -> [{str(), val()}].
info() ->
    call(mbulk(<<"INFO">>), fun list_to_kv_tuple/1).

%% @doc Get the UNIX time stamp of the last successful save to disk
-spec lastsave() -> timestamp().
lastsave() ->
    call(mbulk(<<"LASTSAVE">>)).

%% @doc synchronously save the DB on disk
-spec save() -> 'ok' | error().
save() ->
    call(mbulk(<<"SAVE">>)).

%% @doc synchronously save the DB on disk, then shutdown the server
%% (can't pipeline)
-spec shutdown() -> status_code().
shutdown() ->
    redis_client:shutdown(Client).

%% @doc Make the server a slave of another instance, 
%% or promote it as master
-spec slave_of(iolist(), integer()) -> status_code().
slave_of(Host, Port) when 
    (is_list(Host) orelse is_binary(Host)),
    is_integer(Port) ->
    call(mbulk(<<"SLAVEOF">>, Host, ?N2S(Port))).

%% @doc make the redis from slave into a master instance 
-spec slave_off() -> 'ok'.
slave_off() ->
    call(mbulk(<<"SLAVEOF">>, <<"no">>, <<"one">>)).

%% @doc other unknown commands
command(Args) ->
    call(mbulk_list(Args)).

%%------------------------------------------------------------------------------
%%
%% internal API
%%
%%------------------------------------------------------------------------------

%% get sorted set in range, by index
do_zrange(Cmd, Key, Start, End, false) ->
    call(mbulk(Cmd, Key, ?N2S(Start), ?N2S(End)));
do_zrange(Cmd, Key, Start, End, true) ->
    call(mbulk_list([Cmd, Key, ?N2S(Start), ?N2S(End), <<"WITHSCORES">>]),
        fun(R) -> list_to_kv_tuple(R, fun(S) -> str_to_score(S) end)).

%% get sorted set in range, by score
do_zrange_by_score(Cmd, Key, Min, Max, false) ->
    call(mbulk(Cmd, Key, score_to_str(Min), score_to_str(Max)));
do_zrange_by_score(Cmd, Key, Min, Max, true) ->
    call(mbulk_list([Cmd, Key, 
        score_to_str(Min), score_to_str(Max), <<"WITHSCORES">>]),
        fun(R) -> list_to_kv_tuple(R, fun(S) -> str_to_score(S) end)).

-spec zset_range_score(Key :: key(), Min :: score(), Max :: score(), 
    Start :: index(), Count :: integer(), WithScore :: boolean()) ->
        'null' | [value() | {key(), value()}].
zset_range_score(Key, Min, Max, Start, Count, WithScore) ->
    case WithScore of
        false ->
            call(mbulk_list([<<"ZRANGEBYSCORE">>, Key, 
                score_to_str(Min), 
                score_to_str(Max),
                <<"LIMIT">>,
                ?N2S(Start),
                ?N2S(Count)
                ]));
        true ->
            call(mbulk_list([<<"ZRANGEBYSCORE">>, Key, 
                score_to_str(Min), 
                score_to_str(Max),
                <<"LIMIT">>,
                ?N2S(Start),
                ?N2S(Count),
                <<"WITHSCORES">>
                ]),
            fun(R) -> list_to_kv_tuple(R, fun(S) -> str_to_score(S) end) end)
    end.

do_zset_store(Cmd, Dst, Keys, Weights, Aggregate) when
        Aggregate =:= sum;
        Aggregate =:= min;
        Aggregate =:= max ->
    L = Keys 
    ++ ?IF(Weights =:= [], [], [<<"WEIGHTS">> | Weights])
    ++ [<<"AGGREGATE">>, aggregate_to_str(Aggregate)],
    call(mbulk_list([Cmd, Dst, length(Keys) | L])).

%% call the command
call(Cmd) ->
    call(Cmd, ?NONE).
call(Cmd, Fun) ->
    case Pipeline of
        false ->
            % normal model
            R = redis_client:command(Client, Cmd),
            ?IF(Fun =/= ?NONE, Fun(R), R);
        true ->
            % pipeline model
            add_pipeline_cmd(Cmd, Fun)
    end.

%% get the application vsn
%% return 'undefined' | string().
get_app_vsn() ->
    case application:get_application() of
        undefined ->
            undefined;
        _ ->
            {ok, App} = application:get_application(),
            {ok, Vsn} = application:get_key(App, vsn),
            Vsn
    end.

%%--------------------
%% pipeline internal
%%--------------------
-define(PIPELINE_CMD_KEY, '$pipeline_cmd_key').
-define(PIPELINE_FUN_KEY, '$pipeline_fun_key').

add_pipeline_cmd(Cmd, Fun) ->
    proc_list_add(?PIPELINE_CMD_KEY, Cmd),
    proc_list_add(?PIPELINE_FUN_KEY, Fun).

get_pipeline_cmd() ->
    proc_list_get(?PIPELINE_CMD_KEY).

get_pipeline_fun() ->
    proc_list_get(?PIPELINE_FUN_KEY).

proc_list_add(Key, Val) ->
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, [Val]);
        L ->
            erlang:put(Key, [Val | L])
    end.

proc_list_get(Key) ->
    case erlang:get(Key) of
        undefined ->
            [];
        L ->
            lists:reverse(L)
    end.

proc_list_clear(Key) ->
    erlang:erase(Key).

%%------------------
%% data convert
%%------------------

%% the timeout value
timeout_val(infinity) ->
    "0";
timeout_val(T) when is_integer(T), T >= 0 ->
    ?N2S(T).

%% convert list to n elments tuple list
list_to_n_tuple([], _N) ->
    [];
list_to_n_tuple(L, 1) ->
    L;
list_to_n_tuple(L, N) when N > 1 ->
    {1, [], AccL} =
    lists:foldl(
        fun
            (E, {I, AccPart, AccL}) when I < N ->
                {I+1, [E | AccPart], AccL};
            (E, {I, AccPart, AccL}) when I =:= N ->
                {1, [], [list_to_tuple(lists:reverse([E | AccPart])) | AccL]}
        end,
    {1, [], []}, L),
    lists:reverse(AccL).


aggregate_to_str(sum) -> <<"SUM">>;
aggregate_to_str(min) -> <<"MIN">>;
aggregate_to_str(max) -> <<"MAX">>.

%% convert [{200, 2}, {300, 4}] to 
%% "200 2 300 4"
to_save_str(L) ->
    L2 = 
    [lists:concat([Time, " ", Change])
        || {Time, Change} <- L, is_integer(Time), is_integer(Change)],
    string:join(L2, " ").

%% if the value is empty string
is_empty_str("") -> true;
is_empty_str(<<>>) -> true;
is_empty_str(_) -> false.

%% score convert string for zset
score_to_str('-inf') ->
    <<"-inf">>.
score_to_str('+inf') ->
    <<"+inf">>.
score_to_str({open, S}) ->
    ?N2S(S); 
score_to_str({closed, S}) ->
    [$( | ?N2S(S)];
score_to_str(S) when is_integer(S) ->
    ?N2S(S);
score_to_str(S) when is_float(S) ->
    erlang:float_to_list(S).

%% convert to list
may_single_to_list([H|_] = V) when is_list(H); is_binary(H) -> V;
may_single_to_list(V) -> [V].

%% convert to boolean if the value is possible, otherwise return the value self
int_may_bool(0) -> false;
int_may_bool(1) -> true;
int_may_bool(V) -> V.

%% convert list like [f1, v1, f2, v2] to the key-value tuple
%% [{f1, v1}, {f2, v2}].
list_to_kv_tuple(null) ->
    [];
list_to_kv_tuple(L) ->
    list_to_kv_tuple(L, null).

list_to_kv_tuple([], _Fun) ->
    [];
list_to_kv_tuple(L, Fun) ->
    {odd, null, AccL} =
    lists:foldl(
        fun
            (E, {odd, null, AccL}) ->
                {even, E, AccL};
            (E, {even, EFirst, AccL}) ->
                {odd, null, [{EFirst, do_fun(Fun, E)} | AccL]}
        end,
    {odd, null, []}, L),
    lists:reverse(AccL).

%% call the function on the element
do_fun(null, E) ->
    E;
do_fun(Fun, E) ->
    Fun(E).

%% convert reply to list
may_null_to_list(null) -> [];
may_null_to_list(L) when is_list(L) -> L.

%% string => score
str_to_score(B) when is_binary(B) ->
    str_to_score(binary_to_list(B));
str_to_score(S) when is_list(S) ->
    case catch list_to_integer(S) of
        {'EXIT', _} ->
            list_to_float(S);
        N ->
            N
    end.

