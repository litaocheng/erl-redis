%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng@gmail.com
%%% @doc the interface for redis, all the commands are same with redis.io/commands,
%%%     except for all the function are lowercase of the redis commands
%%%     (this module is parameterized module: Client - the context related client process)
%%%     ref:http://redis.io/commands
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis, [Client]).
-author('litaocheng@gmail.com').
-vsn('0.1').
-include("redis_internal.hrl").

-export([i/0, version/0, db/0]).

%% connection handling
-export([auth/1, ping/0]).

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

%% transaction commands
-export([watch/1, unwatch/0,
        trans_begin/0, trans_commit/0, trans_abort/0]).

%% pubsub commands
-export([subscribe/3, unsubscribe/1, unsubscribe/2, publish/2]).

%% persistence commands
-export([save/0, bg_save/0, lastsave_time/0, shutdown/0, bg_rewrite_aof/0]).

%% remote server commands
-export([info/0, slave_off/0, slave_of/2, config_get/1, config_set/2]).

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

%%------------------------------------------------------------------------------
%% connection handling
%%------------------------------------------------------------------------------

%% @doc simple password authentication
-spec auth(iolist()) -> 'ok' | error().
auth(Pass) ->
    call(mbulk(<<"AUTH">>, Pass)).

%% @doc ping the redis server
-spec ping() -> 'ok' | error().
ping() ->
    call(mbulk(<<"PING">>)).

%%-------
%% keys
%%-------

%% @doc delete keys
%% return the number of keys that were removed
-spec del(Keys :: [key()] | key()) -> 
    integer().
del([H|_] = Key) when is_list(H); is_binary(H) ->
    call(mbulk_list([<<"DEL">> | Key]));
del(Key) when is_binary(Key) ->
    call(mbulk(<<"DEL">>, Key)).

%% @doc test if the key exists
%% O(1)
-spec exists(Key :: key()) -> 
    boolean().
exists(Key) ->
    R = call(mbulk(<<"EXISTS">>, Key)),
    int_may_bool(R).

%% @doc set a timeout on the specified key, after the Time the Key will
%% automatically deleted by server.
%% O(1)
-spec expire(Key :: key(), Time :: second()) -> 
    boolean().
expire(Key, Time) ->
    R = call(mbulk(<<"EXPIRE">>, Key, ?N2S(Time))),
    int_may_bool(R).

%% @doc set a unix timestamp on the specified key, the Key will
%% automatically deleted by server at the TimeStamp in the future.
%% O(1)
-spec expire_at(Key :: key(), TimeStamp :: timestamp()) -> 
    boolean().
expire_at(Key, TimeStamp) ->
    R = call(mbulk(<<"EXPIREAT">>, Key, ?N2S(TimeStamp))),
    int_may_bool(R).

%% @doc return the keys list matching a given pattern
%% O(n)
-spec keys(Pattern :: pattern()) -> 
    [key()].
keys(Pattern) ->
    to_list(call(mbulk(<<"KEYS">>, Pattern))).

%% @doc move a key to anthor db
-spec move(key(), uint()) ->
    boolean().
move(Key, DBIndex) ->
    int_may_bool(call(mbulk(<<"MOVE">>, Key, ?N2S(DBIndex)))).

%% @doc Inspect the internals of Redis objects
%% SubCmd: REFCOUNT, ENCODING, IDLETIME
-spec object(command(), list()) -> integer() | str().
object(SubCmd, Args) ->
    call(mbulk_list([<<"OBJECT">>, SubCmd | Args])).

%% @doc Remove the expiration from a key
-spec persist(key()) -> boolean().
persist(Key) ->
    int_may_bool(call(mbulk("PERSIST", Key))).

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
    int_may_bool(call(mbulk(<<"RENAMENX">>, Key, NewKey))).

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
    L = 
    call(mbulk_list([<<"SORT">>, Key | 
        lists:append([ByPart, LimitPart, GetPart, AscPart, AlphaPart, StorePart])
    ])),
    list_to_n_tuple(L, FieldCount).

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




%% @doc return the nubmer of keys in all the currently selected database
%% O(1)
-spec dbsize() -> count().
dbsize() ->
    call(mbulk(<<"DBSIZE">>)).


%% @doc select the DB with have the specified zero-based numeric index
-spec select(Index :: index()) ->
    'ok' | error().
select(Index) ->
    R = call(mbulk(<<"SELECT">>, ?N2S(Index))),
    case R of
        ok ->
            redis_client:set_selected_db(Client, Index);
        Other ->
            Other
    end.

%% @doc _delete_ all the keys of the currently selected DB
-spec flush_db() -> 'ok'.
flush_db() ->
    call(mbulk(<<"FLUSHDB">>)).
            
%% @doc _delete_ all the keys of existing databases in redis server
-spec flush_all() -> 'ok'.
flush_all() ->
    call(mbulk(<<"FLUSHALL">>)).

%%-----------------------------------------
%% string commands 
%%-----------------------------------------

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
    int_may_bool(call(mbulk_list(L))).

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

%%--------------------------------
%% hashes
%%--------------------------------

%% @doc delete one or more hash fields
-spec hdel(key(), [key()]) -> boolean().
hdel(Key, [H|_] = Field) when is_list(H); is_binary(H) ->
    call(mbulk_list([<<"HDEL">> | Field]));
hdel(Key, Field) ->
    call(mbulk(<<"HDEL">>, Field)).

%% @doc determine if a hash field exists
-spec hexists(key(), key()) -> boolean().
hexists(Key, Field) ->
    int_may_bool(call(mbulk(<<"HEXISTS">>, Key, Field))).

%% @doc get the value of the a hash field
-spec hget(key(), key()) -> val().
hget(Key, Field) ->
    call(mbulk(<<"HGET">>, Key, Field)).

%% @doc get all the fields and values in a hash
-spec hgetall(key()) -> [{key(), str()}].
hgetall(Key) ->
    list_to_kv_tuple(call(mbulk(<<"HGETALL">>, Key))).

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
    int_my_bool(call(mbulk(<<"HSET">>, Key, Field, Val))).

%% @doc Set the value of a hash field, only if the field 
%% does not exist
-spec hsetnx(key(), key(), str()) -> boolean().
hsetnx(Key, Field, Val) ->
    int_may_bool(call(mbulk(<<"HSETNX">>, Key, Field, Val))).

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
    to_list(call(mbulk(<<"LRANGE">>, Key, ?N2S(Start), ?N2S(End)))).

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
    int_may_bool(call(mbulk(<<"SISMEMBER">>, Key, Mem))).

%% @doc get all the members in the set
-spec smembers(key()) -> boolean().
smembers(Key) ->
    call(mbulk(<<"SMEMBERS">>, Key)).

%% @doc Move a member from one set to another
-spec smove(key(), key(), str()) -> boolean().
smove(Src, Dst, Mem) ->
    int_may_bool(call(mbulk(<<"SMOVE">>, Src, Dst, Mem))).

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
    str_to_score(call(mbulk(<<"ZINCRBY">>, Key, ?N2S(N), Mem))).

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

%%------------------------------------------------------------------------------
%% transaction commands 
%%------------------------------------------------------------------------------

%% @doc watch some keys, before the EXEC command in transaction, if the watched keys
%% were changed, the transaction failed, otherwise the transaction success
-spec watch([key()]) -> 'ok'.
watch(Keys) ->
    call(mbulk_list([<<"WATCH">> | Keys])).

%% @doc flush all the watched keys
-spec unwatch() -> 'ok'.
unwatch() ->
    call(mbulk(<<"UNWATCH">>)).

%% @doc transaction begin
-spec trans_begin() -> 'ok' | error().
trans_begin() ->
    call(mbulk(<<"MULTI">>)).

%% @doc transaction commit
-spec trans_commit() -> [any()].
trans_commit() ->
    call(mbulk(<<"EXEC">>)).

%% @doc transaction discard
-spec trans_abort() ->
    'ok'.
trans_abort() ->
    call(mbulk(<<"DISCARD">>)).

%% @doc subscribe to channels
-spec subscribe([channel()], fun(), fun()) ->
    'ok'.
subscribe(Channels, CbSub, CbMsg) ->
    redis_client:subscribe(Client, Channels, CbSub, CbMsg).

%% @doc unsubscribe to all channels
-spec unsubscribe(fun()) ->
    'ok'.
unsubscribe(CbUnSub) ->
    redis_client:unsubscribe(Client, CbUnSub).

%% @doc unsubscribe some channels
-spec unsubscribe([channel()], fun()) ->
    'ok'.
unsubscribe(Channels, CbUnSub) ->
    redis_client:unsubscribe(Client, Channels, CbUnSub).

%% @doc publish message to channel
-spec publish(channel(), str()) ->
    count().
publish(Channel, Msg) ->
    call(mbulk(<<"PUBLISH">>, Channel, Msg)).


%%------------------------------------------------------------------------------
%% persistence commands 
%%------------------------------------------------------------------------------

%% @doc synchronously save the DB on disk
-spec save() -> 'ok' | error().
save() ->
    call(mbulk(<<"SAVE">>)).

%% @doc save the DB in background
-spec bg_save() -> 'ok' | error().
bg_save() ->
    call(mbulk(<<"BGSAVE">>)).

%% @doc return the UNIX time that the last DB save excuted with success
-spec lastsave_time() -> timestamp().
lastsave_time() ->
    call(mbulk(<<"LASTSAVE">>)).

%% @doc synchronously save the DB on disk, then shutdown the server
-spec shutdown() -> 'ok' | error().
shutdown() ->
    redis_client:shutdown(Client).

%% @doc rewrite the append only log in background
-spec bg_rewrite_aof() -> 'ok' | error().
bg_rewrite_aof() ->
    call(mbulk(<<"BGREWRITEAOF">>)).

%%------------------------------------------------------------------------------
%% remote server control commands 
%%------------------------------------------------------------------------------
%% @doc return the info about the server
-spec info() -> [{atom(), value()}].
info() ->
    call(mbulk(<<"INFO">>)).

%% @doc make the redis from slave into a master instance 
-spec slave_off() -> 'ok'.
slave_off() ->
    call(mbulk(<<"SLAVEOF">>, <<"no">>, <<"one">>)).

%% @doc change the slave's master to Host:Port (the old data will be discraded)
%% FIXME it seems in redis-2.0 the salveof command always return ok
-spec slave_of(iolist(), integer()) -> 'ok'.
slave_of(Host, Port) when 
    (is_list(Host) orelse is_binary(Host)),
    is_integer(Port) ->
    call(mbulk(<<"SLAVEOF">>, Host, ?N2S(Port))).

%% @doc retrieve the config info in running redis server
-spec config_get(pattern()) -> 
    [{binary(), any()}].
config_get(Pattern) ->
    KVList = call(mbulk(<<"CONFIG">>, <<"GET">>, Pattern)),
    list_to_kv_tuple(KVList).

%% @doc alter the config info in running redis server
-spec config_set(string(), any()) -> 'ok'.
config_set("save", L) ->
    Str = entry_to_str(L),
    call(mbulk(<<"CONFIG">>, <<"SET">>, <<"save">>, Str));
config_set(Par, Val) when is_list(Par) ->
    ValStr =
    if is_integer(Val) ->
            ?N2S(Val);
        is_list(Val) ->
            Val
    end,
    call(mbulk(<<"CONFIG">>, <<"SET">>, Par, ValStr)).


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
    L = call(mbulk_list([Cmd, Key, ?N2S(Start), ?N2S(End), <<"WITHSCORES">>])),
    list_to_kv_tuple(L, fun(S) -> str_to_score(S) end).

%% get sorted set in range, by score
do_zrange_by_score(Cmd, Key, Min, Max, false) ->
    call(mbulk(Cmd, Key, score_to_str(Min), score_to_str(Max)));
do_zrange_by_score(Cmd, Key, Min, Max, true) ->
    L = call(mbulk_list([Cmd, Key, 
        score_to_str(Min), score_to_str(Max), <<"WITHSCORES">>])),
    list_to_kv_tuple(L, fun(S) -> str_to_score(S) end).

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
            L = call(mbulk_list([<<"ZRANGEBYSCORE">>, Key, 
                score_to_str(Min), 
                score_to_str(Max),
                <<"LIMIT">>,
                ?N2S(Start),
                ?N2S(Count),
                <<"WITHSCORES">>
                ])),
            list_to_kv_tuple(L, fun(S) -> str_to_score(S) end)
    end.





do_zset_store(Cmd, Dst, Keys, Weights, Aggregate) when
        Aggregate =:= sum;
        Aggregate =:= min;
        Aggregate =:= max ->
    L = Keys 
    ++ ?IF(Weights =:= [], [], [<<"WEIGHTS">> | Weights])
    ++ [<<"AGGREGATE">>, aggregate_to_str(Aggregate)],
    call(mbulk_list([Cmd, Dst, length(Keys) | L])).

call(Cmd) ->
    redis_client:command(Client, Cmd).

call(Cmd, Timeout) ->
    redis_client:command(Client, Cmd, Timeout).

%% the timeout value
timeout_val(infinity) ->
    "0";
timeout_val(T) when is_integer(T), T >= 0 ->
    ?N2S(T).

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
entry_to_str(L) ->
    L2 = 
    [ lists:concat([Time, " ", Change])
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

%%------------------
%% handle the reply
%%------------------

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
to_list(null) -> [];
to_list(L) when is_list(L) -> L.

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

