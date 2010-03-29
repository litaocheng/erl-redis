# README
erl-redis is redis client library for erlang.
you can get much more information about redis at http://github.com/antirez/redis.

## Install
At first, you must install and start the redis server.

Then let us do some tests, you must start three redis-server bind 6379, 6380, 16379 in 
localhost for test usage. after above, we can do the test:
    $ make test

if some test cases not passed, you need to do some toubleshooting: update your redis-server 
to new version or disable some redis commands.

if all the test case passed, now you can install the redis client:

    $ make
    $ make install

the make install statement will install the redis library into the 
$ERL_TOP/lib/ directory (kernel, stdlib also in that directory).
now you can use the redis client in your application.
(NOTE: we recommend you first run make test when you want to use erl-redis client, because
different redis server versions have some trivial differences in protocol.)


## Usage

the redis application is working like the standard erlang library. e.g. sasl, crypto, inets.
you must start the redis application by calling application:start(redis) when you want to use redis client.
the erl redis application manages the connections with redis server automatically.

the erl-redis client now supports two types of mode:'single' server and 'dist' servers. it also support the
group of servers, use the group we can access different redis servers with different usage.
for example, we has two groups: cache group and user_info group. the cache group has only one redis server,
it not save data into the disk. the user_info group has serval redis servers  in the 'dist' mode. 

single:
    redis_app:start(), 
    ok = redis_app:single_server(localhost, 6379, 5),  % connection pool is 5 
    Redis = redis_app:client(single),
    Redis:exists("key"). 

dist:
    redis_app:start(),
    Servers = [{localhost, 6379, 3}, {localhost, 6380, 5}],
    ok = redis_app:multi_server(Servers, "pwd"),
    Redis = redis_app:client(dist),
    Redis:exists("key").

group:
    redis_app:start(),
    Server = {localhost, 16379, 3},
    Servers = [{localhost, 6379, 3}, {localhost, 6380, 5}],
    ok = redis_app:group_server(cache, single, Server, "pwd"),
    ok = redis_app:group_server(user_info, dist, Servers, "pwd"),
    RedisCache = redis_app:client(cache, single),
    RedisUser = redis_app:client(user_info, dist),
    RedisCache:exists("key"),
    RedisUser:exists("key").
    

you can find more usage info in the test/redis_SUITE.erl module
