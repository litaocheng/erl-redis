%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng@gmail.com
%%% @doc the interface for redis, this module is parameterized module:
%%%     PClient - the context related client process
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(redis, [PClient]).
-author('litaocheng@gmail.com').
-vsn('0.1').
-include("redis_internal.hrl").

-export([i/0, version/0, db/0]).

%% generic commands
-export([ping/0, exists/1, delete/1, multi_delete/1, type/1, keys/1,
        random_key/0, rename/2, rename_not_exists/2, dbsize/0, 
        expire/2, expire_at/2, ttl/1, select/1, move/2, 
        flush_db/0, flush_all/0]).

%% string commands
-export([set/2, set/3, get/1, getset/2, multi_get/1, not_exists_set/2, multi_set/1,
        multi_set_not_exists/1, incr/1, incr/2, decr/1, decr/2,
        append/2, substr/3]).

%% list commands
-export([list_push_tail/2, list_push_head/2, list_len/1, list_range/3, 
        list_trim/3, list_index/2, list_set/3, 
        list_rm/2, list_rm_from_head/3, list_rm_from_tail/3,
        list_pop_head/1, list_pop_tail/1, list_tail_to_head/2,
        list_block_pop_head/2, list_block_pop_tail/2]).

%% set commands
-export([set_add/2, set_rm/2, set_pop/1, set_move/3, set_len/1, set_is_member/2,
        set_inter/1, set_inter_store/2, set_union/1, set_union_store/2,
        set_diff/2, set_diff_store/3, set_members/1, set_random_member/1]).

%% sorted set commands
-export([zset_add/3, zset_rm/2, zset_incr/3, zset_index/2, zset_reverse_index/2,
        zset_range_index/4, zset_range_index_reverse/4, 
        zset_range_score/4, zset_range_score/6, zset_len/1, zset_score/2, 
        zset_rm_by_index/3, zset_rm_by_score/3, 
        zset_union_store/2, zset_union_store/4,
        zset_inter_store/2, zset_inter_store/4]).


%% hash commands
-export([hash_set/3, hash_set_not_exists/3, hash_multi_set/2,
        hash_get/2, hash_multi_get/2, 
        hash_incr/3, hash_del/2, hash_exists/2, hash_len/1, 
        hash_keys/1, hash_vals/1, hash_all/1]).

%% sort commands
-export([sort/2]).

%% transaction commands
-export([trans_begin/0, trans_commit/0, trans_abort/0]).

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
    end.
-define(MULTI_REPLY_OK,
    case BadServers of
        [] ->
            ok;
        [_|_] ->
            {error, BadServers}
    end.

-define(CALL, redis_client:send(PClient)).

%% @doc show stats info in the stdout
-spec i() -> 'ok'.
i() ->
    ok.

%% @doc return the redis version info
-spec version() -> string().
version() ->
    get_app_vsn().

%% @doc return the currently selected db
-spec db() -> index().
db() ->
    redis_manager:get_selected_db(PClient).

%%------------------------------------------------------------------------------
%% generic commands
%%------------------------------------------------------------------------------

%% @doc ping the redis server
-spec ping() -> 'ok' | {'error', [atom()]}.
ping() ->
    call(mbulk(<<"PING">>)).

%% @doc test if the specified key exists
%% O(1)
-spec exists(Key :: key()) -> 
    boolean().
exists(Key) ->
    R = call(mbulk(<<"EXISTS">>, Key)),
    int_may_bool(R).

%% @doc remove the specified key, return ture if deleted, 
%% otherwise return false
%% O(1)
-spec delete(Keys :: [key()]) -> 
    boolean().
delete(Key) ->
    R = call(mbulk(<<"DEL">>, Key)),
    int_may_bool(R).

%% @doc remove the specified keys, return the number of
%% keys removed.
%% O(1)
-spec multi_delete(Keys :: [key()]) -> 
    non_neg_integer().
multi_delete(Keys) ->
    call(mbulk_list([<<"DEL">> | Keys])).

%% @doc return the type of the value stored at key
%% O(1)
-spec type(Key :: key()) -> 
    value_type().
type(Key) ->
    call(mbulk(<<"TYPE">>, Key)).

%% @doc return the keys list matching a given pattern
%% O(n)
-spec keys(Pattern :: pattern()) -> 
    [key()].
keys(Pattern) ->
    case call(mbulk(<<"KEYS">>, Pattern)) of
        null ->
            [];
        Keys ->
            Keys
    end.

%% @doc return a randomly selected key from the currently selected DB
%% O(1)
-spec random_key() -> 
    key() | null().
random_key() ->
    call(mbulk(<<"RANDOMKEY">>)).

%% @doc atmoically renames key OldKey to NewKey
%% O(1)
-spec rename(OldKey :: key(), NewKey :: key()) -> 
    'ok'.
rename(OldKey, NewKey) ->
    call(mbulk(<<"RENAME">>, OldKey, NewKey)).

%% @doc  rename oldkey into newkey  but fails if the destination key newkey already exists. 
%% O(1)
-spec rename_not_exists(OldKey :: key(), NewKey :: key()) -> 
    boolean().
rename_not_exists(OldKey, NewKey) ->
    R = call(mbulk(<<"RENAMENX">>, OldKey, NewKey)),
    int_may_bool(R).

%% @doc return the nubmer of keys in all the currently selected database
%% O(1)
-spec dbsize() -> 
    integer().
dbsize() ->
    call(mbulk(<<"DBSIZE">>)).

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

%% @doc return the remaining time to live in seconds of a key that
%% has an EXPIRE set
%% O(1)
-spec ttl(Key :: key()) -> non_neg_integer().
ttl(Key) ->
    call(mbulk(<<"TTL">>, Key)).

%% @doc select the DB with have the specified zero-based numeric index
-spec select(Index :: index()) ->
    'ok'.
select(Index) ->
    R = call(mbulk(<<"SELECT">>, ?N2S(Index))),
    case R of
        ok ->
            redis_client:set_selected_db(PClient, Index);
        Other ->
            Other
    end.

%% @doc move the specified key  from the currently selected DB to the specified
%% destination DB
-spec move(Key :: key(), DBIndex :: index()) ->
    boolean().
move(Key, DBIndex) ->
    R = call(mbulk(<<"MOVE">>, Key, ?N2S(DBIndex))),
    int_may_bool(R).

%% @doc delete all the keys of the currently selected DB
-spec flush_db() -> 'ok'.
flush_db() ->
    call(mbulk(<<"FLUSHDB">>)).
            
-spec flush_all() -> 'ok'.
flush_all() ->
    call(mbulk(<<"FLUSHALL">>)).

%%------------------------------------------------------------------------------
%% string commands 
%%------------------------------------------------------------------------------

%% @doc set the string as value of the key
%% O(1)
-spec set(Key :: key(), Val :: str()) ->
    'ok'.
set(Key, Val) ->
    call(mbulk(<<"SET">>, Key, Val)).

%% @doc set the string as value of the key with expired time
%% O(1)
-spec set(key(), str(), second()) ->
    'ok'.
set(Key, Val, Expire) ->
    call(mbulk(<<"SETEX">>, Key, ?N2S(Expire), Val)).
    
%% @doc get the value of specified key
%% O(1)
-spec get(Key :: key()) ->
    null() | binary().
get(Key) ->
    call(mbulk(<<"GET">>, Key)).

%% @doc atomatic set the value and return the old value
%% O(1)
-spec getset(Key :: key(), Val :: str()) ->
    null() | binary(). 
getset(Key, Val) ->
    call(mbulk(<<"GETSET">>, Key, Val)).

%% @doc get the values of all the specified keys
%% O(1)
-spec multi_get(Keys :: [key()]) ->
    [string() | null()].
multi_get(Keys) ->
    call(mbulk_list([<<"MGET">> | Keys])).

%% @doc set value, if the key already exists no operation is performed
%% O(1)
-spec not_exists_set(Key :: key(), Val :: string()) -> 
    boolean().
not_exists_set(Key, Val) ->
    R = call(mbulk(<<"SETNX">>, Key, Val)),
    int_may_bool(R).

%% @doc set the respective keys to respective values
%% O(1)
-spec multi_set(KeyVals :: [{key(), str()}]) ->
    'ok'.
multi_set(KeyVals) ->
    L = [<<"MSET">> | lists:append([[K, V] || {K, V} <- KeyVals])],
    call(mbulk_list(L)).

%% @doc set the respective keys to respective values, either all the
%% key-value paires or not none at all are set.
%% O(1)
-spec multi_set_not_exists(KeyVals :: [{key(), str()}]) ->
    boolean().
multi_set_not_exists(KeyVals) ->
    L = [<<"MSETNX">> | lists:append([[K, V] || {K, V} <- KeyVals])],
    R = call(mbulk_list(L)),
    int_may_bool(R).

%% @doc increase the integer value of key
%% O(1)
-spec incr(Key :: key()) -> 
    integer().
incr(Key) ->
    call(mbulk(<<"INCR">>, Key)).

%% @doc increase the integer value of key by integer
%% O(1)
-spec incr(Key :: key(), N :: integer()) -> 
    integer().
incr(Key, N) ->
    call(mbulk(<<"INCRBY">>, Key, ?N2S(N))).

%% @doc decrease the integer value of key
%% O(1)
-spec decr(Key :: key()) -> 
    integer().
decr(Key) ->
    call(mbulk(<<"DECR">>, Key)).

%% @doc decrease the integer value of key by integer
%% O(1)
-spec decr(Key :: key(), N :: integer()) -> 
    integer().
decr(Key, N) ->
    call(mbulk(<<"DECRBY">>, Key, ?N2S(N))).

%% @doc append the specified string to the string stored at key,
%%      return the new string length
%% O(1)
-spec append(key(), value()) -> 
    integer().
append(Key, Str) ->
    call(mbulk(<<"APPEND">>, Key, Str)).

%% @doc return a substring of a larger string, both Start and End are inclusive
%% O(1)
-spec substr(key(), integer(), integer()) -> 
    integer().
substr(Key, Start, End) when is_integer(Start), is_integer(End) ->
    call(mbulk(<<"SUBSTR">>, Key, ?N2S(Start), ?N2S(End))).


%%------------------------------------------------------------------------------
%% list commands
%%------------------------------------------------------------------------------

%% @doc add the string Val to the tail of the list stored at Key
%% returnt he current list length
%% O(1)
-spec list_push_tail(Key :: key(), Val :: str()) ->
    integer().
list_push_tail(Key, Val) ->
    call(mbulk(<<"RPUSH">>, Key, Val)).

%% @doc add the string Val to the head of the list stored at Key
%% O(1)
-spec list_push_head(Key :: key(), Val :: str()) ->
    'ok' | error().
list_push_head(Key, Val) ->
    call(mbulk(<<"LPUSH">>, Key, Val)).

%% @doc return the length of the list
%% O(1)
-spec list_len(Key :: key()) ->
    length() | error().
list_len(Key) ->
    call(mbulk(<<"LLEN">>, Key)).

%% @doc return the specified elements of the list, Start, End are zero-based
%% O(n)
-spec list_range(Key :: key(), Start :: index(), End :: index()) ->
    [value()].
list_range(Key, Start, End) ->
    call(mbulk(<<"LRANGE">>, Key, ?N2S(Start), ?N2S(End))).

%% @doc trim an existing list, it will contains the elements specified by 
%% Start End
%% O(n)
-spec list_trim(Key :: key(), Start :: index(), End :: index()) ->
    'ok' | error().
list_trim(Key, Start, End) ->
    call(mbulk(<<"LTRIM">>, Key, ?N2S(Start), ?N2S(End))).

%% @doc return the element at Idx in the list(zero-based)
%% O(n)
-spec list_index(Key :: key(), Idx :: index()) ->
    value().
list_index(Key, Idx) ->
    call(mbulk(<<"LINDEX">>, Key, ?N2S(Idx))).

%% @doc set the list element at Idx with the new value Val
%% O(n)
-spec list_set(Key :: key(), Idx :: index(), Val :: str()) ->
    'ok' | error().
list_set(Key, Idx, Val) ->
    call(mbulk(<<"LSET">>, Key, ?N2S(Idx), Val)).

%% @doc remove all the elements in the list whose value are Val
%% O(n)
-spec list_rm(Key :: key(), Val :: str()) ->
    integer().
list_rm(Key, Val) ->
    call(mbulk(<<"LREM">>, Key, "0", Val)).

%% @doc remove the first N occurrences of the value from head to
%% tail in the list
%% O(n)
-spec list_rm_from_head(Key :: key(), N :: pos_integer(), Val :: str()) ->
    integer().
list_rm_from_head(Key, N, Val) ->
    call(mbulk(<<"LREM">>, Key, ?N2S(N), Val)).

%% @doc remove the first N occurrences of the value from tail to
%% head in the list
%% O(n)
-spec list_rm_from_tail(Key :: key(), N :: pos_integer(), Val :: str()) ->
    integer().
list_rm_from_tail(Key, N, Val) ->
    call(mbulk(<<"LREM">>, Key, ?N2S(-N), Val)).

%% @doc atomically return and remove the first element of the list
%% O(1)
-spec list_pop_head(Key :: key()) ->
    value().
list_pop_head(Key) ->
    call(mbulk(<<"LPOP">>, Key)).

%% @doc atomically return and remove the last element of the list
%% O(1)
-spec list_pop_tail(Key :: key()) ->
    value().
list_pop_tail(Key) ->
    call(mbulk(<<"RPOP">>, Key)).

%% @doc atomically remove the last element of the SrcKey list and push
%% the element into the DstKey list
%% O(1)
-spec list_tail_to_head(SrcKey :: key(), DstKey :: key()) ->
    'ok'.
list_tail_to_head(SrcKey, DstKey) ->
    call(mbulk(<<"RPOPLPUSH">>, SrcKey, DstKey)).

%% @doc blocking list_pop_head
%% O(1)
-spec list_block_pop_head([key()], timeout()) ->
    value().
list_block_pop_head(Keys, Timeout) ->
    TimeStr = ?N2S(timeout_val(Timeout)),
    call(mbulk([<<"BLPOP">> | (Keys ++ [TimeStr])])).

%% @doc blocking list_pop_tail
%% O(1)
-spec list_block_pop_tail([key()], timeout()) ->
    value().
list_block_pop_tail(Keys, Timeout) ->
    TimeStr = ?N2S(timeout_val(Timeout)),
    call(mbulk([<<"BRPOP">> | (Keys ++ [TimeStr])])).

%%------------------------------------------------------------------------------
%% set commands (in set all members is distinct)
%%------------------------------------------------------------------------------

%% @doc add Mem to the set stored at Key
%% O(1)
-spec set_add(Key :: key(), Mem :: str()) ->
    boolean().
set_add(Key, Mem) ->
    R = call(mbulk(<<"SADD">>, Key, Mem)),
    int_may_bool(R).

%% @doc remove the specified member from the set
-spec set_rm(Key :: key(), Mem :: str()) ->
    boolean().
set_rm(Key, Mem) ->
    R = call(mbulk(<<"SREM">>, Key, Mem)),
    int_may_bool(R).

%% @doc remove a random member from the set, returning it as return value
%% O(1)
-spec set_pop(Key :: key()) ->
    value().
set_pop(Key) ->
    call(mbulk(<<"SPOP">>, Key)).

%% @doc atomically move the member form one set to another
%% O(1)
-spec set_move(Src :: key(), Dst :: key(), Mem :: str()) ->
    boolean().
set_move(Src, Dst, Mem) ->
    R = call(mbulk(<<"SMOVE">>, Src, Dst, Mem)),
    int_may_bool(R).

%% @doc return the number of elements in set
%% O(1)
-spec set_len(Key :: key()) ->
    integer().
set_len(Key) ->
    call(mbulk(<<"SCARD">>, Key)).

%% @doc test if the specified value is the member of the set
%% O(1)
-spec set_is_member(Key :: key(), Mem :: str()) ->
    boolean().
set_is_member(Key, Mem) ->
    R = call(mbulk(<<"SISMEMBER">>, Key, Mem)),
    int_may_bool(R).

%% @doc return the intersection between the sets
%% O(N*M)
-spec set_inter(Keys :: [key()]) ->
    [value()].
set_inter(Keys) ->
    call(mbulk_list([<<"SINTER">> | Keys])).

%% @doc compute the intersection between the sets and save the 
%% resulting to new set
-spec set_inter_store(Dst :: key(), Keys :: [key()]) ->
    'ok' | error().
set_inter_store(Dst, Keys) ->
    call(mbulk_list([<<"SINTERSTORE">>, Dst | Keys])).

%% @doc  return the union of all the sets
%% O(n)
-spec set_union(Keys :: [key()]) ->
    [value()].
set_union(Keys) ->
    call(mbulk_list([<<"SUNION">> | Keys])).

%% @doc compute the union between the sets and save the 
%% resulting to new set
%% O(n)
-spec set_union_store(Dst :: key(), Keys :: [key()]) ->
    'ok' | error().
set_union_store(Dst, Keys) ->
    call(mbulk_list([<<"SUNIONSTORE">>, Dst | Keys])).

%% @doc return the difference between the First set and all the other sets
%% o(n)
-spec set_diff(First :: key(), Keys :: [key()]) ->
    [value()].
set_diff(First, Keys) ->
    call(mbulk_list([<<"SDIFF">>, First | Keys])).

%% @doc compute the difference between the sets and save the 
%% resulting to new set
%% O(n)
-spec set_diff_store(Dst :: key(), First :: key(), Keys :: [key()]) ->
    'ok' | error().
set_diff_store(Dst, First, Keys) ->
    call(mbulk_list([<<"SDIFFSTORE">>, Dst, First | Keys])).  

%% @doc return all the members in the set
%% O(1)
-spec set_members(Key :: key()) ->
    [value()].
set_members(Key) ->
    call(mbulk(<<"SMEMBERS">>, Key)).

%% @doc return a random member from the set
%% O(1)
-spec set_random_member(Key :: key()) ->
    value().
set_random_member(Key) ->
    call(mbulk(<<"SRANDMEMBER">>, Key)).

%%------------------------------------------------------------------------------
%% sorted set commands 
%%------------------------------------------------------------------------------

%% @doc add Mem to the sorted set stored at Key
%% O(log(N))
-spec zset_add(Key :: key(), Mem :: str(), Score :: score()) ->
    boolean().
zset_add(Key, Mem, Score) ->
    R = call(mbulk(<<"ZADD">>, Key, ?N2S(Score), Mem)),
    int_may_bool(R).

%% @doc remove the specified member from the sorted set
%% O(log(N))
-spec zset_rm(Key :: key(), Mem :: str()) ->
    boolean().
zset_rm(Key, Mem) ->
    R = call(mbulk(<<"ZREM">>, Key, Mem)),
    int_may_bool(R).

%% @doc If the member already exists increase its score, 
%% otherwise add the member setting N as score
%% O(log(N))
-spec zset_incr(Key :: key(), Mem :: str(), N :: integer()) ->
    score().
zset_incr(Key, Mem, N) ->
    R = call(mbulk(<<"ZINCRBY">>, Key, ?N2S(N), Mem)),
    string_to_score(R).

%% @doc return the index(rank) of member in the sorted set, the scores being
%% ordered from low to high
%% O(log(N))
%% Reids >= 1.3.4
-spec zset_index(Key :: key(), Mem :: key()) ->
    integer().
zset_index(Key, Mem) ->
    call(mbulk(<<"ZRANK">>, Key, Mem)).

%% @doc return the index(rank) of member in the sorted set, the scores being 
%% ordered from high to low
%% O(log(N))
%% Reids >= 1.3.4
-spec zset_reverse_index(Key :: key(), Mem :: key()) ->
    integer().
zset_reverse_index(Key, Mem) ->
    call(mbulk(<<"ZREVRANK">>, Key, Mem)).

%% @doc return a range of elements from the sorted set
%% O(log(N))+O(M)
-spec zset_range_index(Key :: key(), Start :: index(), End :: index(), 
    WithScore :: boolean()) -> 
        'null' | [value() | {key(), value()}].
zset_range_index(Key, Start, End, WithScore) ->
    do_zset_range_index(<<"ZRANGE">>, Key, Start, End, WithScore).

%% @doc return a range of elements form the sorted set, like zset_range_index, but 
%% the sorted set is ordered in traversed in reverse order.
-spec zset_range_index_reverse(Key :: key(), Start :: index(), End :: index(),
    WithScore :: boolean()) -> 
        'null' | [value() | {key(), value()}].
zset_range_index_reverse(Key, Start, End, WithScore) ->
    do_zset_range_index(<<"ZREVRANGE">>, Key, Start, End, WithScore).

%% @doc return all elements with score in the specified scope
-spec zset_range_score(Key :: key(), Min :: score(), Max :: score(), 
    WithScore :: boolean()) -> 
        'null' | [value() | {key(), value()}].
zset_range_score(Key, Min, Max, WithScore) ->
    case WithScore of
        false ->
            call(mbulk(<<"ZRANGEBYSCORE">>, Key, 
                score_to_string(Min), score_to_string(Max)));
        true ->
            L = call(mbulk_list([<<"ZRANGEBYSCORE">>, Key, 
                score_to_string(Min), score_to_string(Max), <<"WITHSCORES">>])),
            list_to_kv_tuple(L, fun(S) -> string_to_score(S) end)
    end.

-spec zset_range_score(Key :: key(), Min :: score(), Max :: score(), 
    Start :: index(), Count :: integer(), WithScore :: boolean()) ->
        'null' | [value() | {key(), value()}].
zset_range_score(Key, Min, Max, Start, Count, WithScore) ->
    case WithScore of
        false ->
            call(mbulk_list([<<"ZRANGEBYSCORE">>, Key, 
                score_to_string(Min), 
                score_to_string(Max),
                <<"LIMIT">>,
                ?N2S(Start),
                ?N2S(Count)
                ]));
        true ->
            L = call(mbulk_list([<<"ZRANGEBYSCORE">>, Key, 
                score_to_string(Min), 
                score_to_string(Max),
                <<"LIMIT">>,
                ?N2S(Start),
                ?N2S(Count),
                <<"WITHSCORES">>
                ])),
            list_to_kv_tuple(L, fun(S) -> string_to_score(S) end)
    end.

%% @doc return the number of elements in sorted set
-spec zset_len(Key :: key()) ->
    integer().
zset_len(Key) ->
    call(mbulk(<<"ZCARD">>, Key)).

%% @doc return the score of the element
-spec zset_score(Key :: key(), Mem :: key()) ->
    'null' | score().
zset_score(Key, Mem) ->
    call(mbulk(<<"ZSCORE">>, Key, Mem)).

%% @doc remove the elements in the sorted set with index between start and end
-spec zset_rm_by_index(Key :: key(), Start :: index(), End :: index()) ->
    integer().
zset_rm_by_index(Key, Start, End) ->
    call(mbulk(<<"ZREMRANGEBYRANK">>, Key, ?N2S(Start), ?N2S(End))).

%% @doc remove the elements in the sorted set with score between start and end
-spec zset_rm_by_score(Key :: key(), Min :: score(), Max :: score()) ->
    integer().
zset_rm_by_score(Key, Min, Max) ->
    call(mbulk(<<"ZREMRANGEBYSCORE">>, Key, ?N2S(Min), ?N2S(Max))).

%% @doc return the intersection between the sorted sets, and save the 
%% resulting to new sorted set.
-spec zset_union_store(key(), [key()]) ->
    integer().
zset_union_store(Dst, Keys) ->
    call(mbulk_list([<<"ZUNIONSTORE">>, Dst, length(Keys) | Keys])).

zset_union_store(Dst, Keys, Weights, Aggregate) ->
    do_zset_store(<<"ZUNIONSTORE">>, Dst, Keys, Weights, Aggregate).

%% @doc calculate the intersection between the sorted sets, and save the
%% resulting to new sroted set with the key Dst.
-spec zset_inter_store(key(), [key()]) ->
    integer().
zset_inter_store(Dst, Keys) ->
    call(mbulk_list([<<"ZINTERSTORE">>, Dst, length(Keys) | Keys])).

zset_inter_store(Dst, Keys, Weights, Aggregate) ->
    do_zset_store(<<"ZINTERSTORE">>, Dst, Keys, Weights, Aggregate).

%%------------------------------------------------------------------------------
%% hash commands 
%%------------------------------------------------------------------------------

%% @doc set specified hash filed with Val
%% O(1)
-spec hash_set(Key :: key(), field(), Val :: str()) ->
    boolean().
hash_set(Key, Field, Val) ->
    R = call(mbulk(<<"HSET">>, Key, Field, Val)),
    int_may_bool(R).

%% @doc set specified hash filed with Val, 
%% if the field exist no operation is performed
%% O(1)
-spec hash_set_not_exists(key(), field(), str()) -> boolean().
hash_set_not_exists(Key, Field, Val) ->
    R = call(mbulk(<<"HSETNX">>, Key, Field, Val)),
    int_may_bool(R).


%% @doc set the hash fileds with the respective values,
%%  if hash not exists, a new one will create
%% O(N)
-spec hash_multi_set(key(), [{field(), str()}]) -> 'ok'.
hash_multi_set(Key, FieldVals) ->
    L = [<<"HMSET">>, Key | lists:append([[F, V] || {F, V} <- FieldVals])],
    call(mbulk_list(L)).

%% @doc retrieve the value of the specified hash field
%% O(1)
-spec hash_get(Key :: key(), Field :: field()) ->
    value().
hash_get(Key, Field) ->
    call(mbulk(<<"HGET">>, Key, Field)).


%% @doc get the values of all specified fields
%% O(1)
-spec hash_multi_get(key(), [field()]) ->
    [value() | 'null'].
hash_multi_get(Key, Fields) ->
    call(mbulk_list([<<"HMGET">>, Key | Fields])).


%% @doc If the field already exists increase its score, 
%% otherwise add the field with N as score
%% O(log(N))
-spec hash_incr(key(), field(), integer()) ->
    score().
hash_incr(Key, Field, N) ->
    call(mbulk(<<"HINCRBY">>, Key, Field, ?N2S(N))).

%% @doc remove the sepcified field from the hash
%% O(1)
-spec hash_del(Key :: key(), Field :: field()) ->
    boolean().
hash_del(Key, Field) ->
    R = call(mbulk(<<"HDEL">>, Key, Field)),
    int_may_bool(R).

%% @doc test if the field exists in the hash
%% O(1)
-spec hash_exists(Key :: key(), Field :: field()) ->
    boolean().
hash_exists(Key, Field) ->
    R = call(mbulk(<<"HEXISTS">>, Key, Field)),
    int_may_bool(R).

%% @doc return the number of items in hash
%% O(1)
-spec hash_len(Key :: key()) -> integer().
hash_len(Key) ->
    call(mbulk(<<"HLEN">>, Key)).

%% @doc return all the fields in hash
%% O(n)
-spec hash_keys(Key :: key()) -> [value()].
hash_keys(Key) ->
    call(mbulk(<<"HKEYS">>, Key)).

%% @doc return all the values in hash
-spec hash_vals(Key :: key()) -> [value()].
hash_vals(Key) ->
    call(mbulk(<<"HVALS">>, Key)).

%% @doc return a tuple list include both the fields and values in hash
-spec hash_all(key()) -> [tuple()].
hash_all(Key) ->
    KFList = call(mbulk(<<"HGETALL">>, Key)),
    list_to_kv_tuple(KFList).

%%------------------------------------------------------------------------------
%% sort commands
%%------------------------------------------------------------------------------

%% @doc sort a list or set accordingly to the parameters
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

    ByPart = 
    case By of
        "" ->
            [];
        _ ->
            [<<"BY">>, By]
    end,

    LimitPart = 
    case Limit of
        null ->
            [];
        {Start, End} ->
            [<<"LIMIT">>, ?N2S(Start), ?N2S(End)]
    end,

    {FieldCount, GetPart} =
    case Get of
        [] ->
            {1, []};
        _ ->
            {length(Get), lists:append([[<<"GET">>, P] || P <- Get])}
    end,

    AscPart =
    case Asc of
        true ->
            [];
        false ->
            [<<"DESC">>]
    end,

    AlphaPart =
    case Alpha of
        false ->
            [];
        true ->
            [<<"ALPHA">>]
    end,

    StorePart =
    case Store of
        "" ->
            [];
        _ ->
            [<<"STORE">>, Store]
    end,

    L = 
    call(mbulk_list([<<"SORT">>, Key | 
        lists:append([ByPart, LimitPart, GetPart, AscPart, AlphaPart, StorePart])
    ])),
    list_to_n_tuple(L, FieldCount).

%%------------------------------------------------------------------------------
%% transaction commands 
%%------------------------------------------------------------------------------

%% @doc transaction begin
-spec trans_begin() -> 'ok' | {'error', any()}.
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
    redis_client:subscribe(PClient, Channels, CbSub, CbMsg).

%% @doc unsubscribe to all channels
-spec unsubscribe(fun()) ->
    'ok'.
unsubscribe(CbUnSub) ->
    redis_client:unsubscribe(PClient, CbUnSub).

%% @doc unsubscribe some channels
-spec unsubscribe([channel()], fun()) ->
    'ok'.
unsubscribe(Channels, CbUnSub) ->
    redis_client:unsubscribe(PClient, Channels, CbUnSub).

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
-spec shutdown() -> 'ok' | {'error', any()}.
shutdown() ->
    redis_client:shutdown(PClient).

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

%% get sorted set range by index
do_zset_range_index(Cmd, Key, Start, End, WithScore) ->
    case WithScore of
        true ->
            L = call(mbulk_list([Cmd, Key, ?N2S(Start), ?N2S(End), <<"WITHSCORES">>])),
            list_to_kv_tuple(L, fun(S) -> string_to_score(S) end);
        false ->
            call(mbulk(Cmd, Key, ?N2S(Start), ?N2S(End)))
    end.

do_zset_store(Cmd, Dst, Keys, Weights, Aggregate) when
        Aggregate =:= sum;
        Aggregate =:= min;
        Aggregate =:= max ->
    call(mbulk_list([Cmd, Dst, length(Keys) | 
                (Keys 
                    ++ [<<"WEIGHTS">> | Weights] 
                    ++ [<<"AGGREGATE">>, aggregate_to_str(Aggregate)])]
        )).

call(Cmd) ->
    redis_client:send(PClient, Cmd).

%% convert score to string
score_to_string(S) when is_integer(S) ->
    ?N2S(S);
score_to_string(S) when is_list(S) ->
    S.

%% string => score
string_to_score(B) when is_binary(B) ->
    string_to_score(binary_to_list(B));
string_to_score(S) when is_list(S) ->
    case catch list_to_integer(S) of
        {'EXIT', _} ->
            list_to_float(S);
        N ->
            N
    end.

%% the timeout value
timeout_val(infinity) ->
    0;
timeout_val(T) when is_integer(T) ->
    T.

%% convert to boolean if the value is possible, otherwise return the value self
int_may_bool(0) -> false;
int_may_bool(1) -> true;
int_may_bool(V) -> V.

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

%% convert list like [f1, v1, f2, v2] to the key-value tuple
%% [{f1, v1}, {f2, v2}].
list_to_kv_tuple(null) ->
    null;
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
