# README
erl-redis is redis client library for erlang.
you can get much more information about redis at http://github.com/antirez/redis.

## Install
At first, you must install and start the redis server.

Then let us do some tests, you must start three redis-server bind 6379 in 
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

the redis client includes four source files: `redis.erl`, `redis_client.erl`, `redis_conn_sup.erl`,
`redis_proto.erl`. In this version, we do not support the multiple redis servers with some
hashing algorithms, because redis is not used as an cache (just like memcached), it support
the persistent storage, so about the cluster we need to think so much cases. So we just implement
a simple redis client accrodding to the redis protocol. we will go along with the redis-cluster.

the erl-redis now support all the commands in redis-1.3.5, all the commands now implemented with
the new mbulk protocol. we have complete the follow commands:

 * generic commands
 * string commands
 * list commands
 * set commands
 * sorted set commands
 * hash commands
 * transaction(MULT/DISCARD/EXEC) commands
 * sort commands
 * pubsub commands
 * persistence commands

all the API is in the src/redis.erl.

### usage scenarios
e.g. 1 (no registered name)
    {ok, Pid} = redis_client:start(Host, Port, ""),
    Redis = redics_client:handler(Pid),
    Redis:set("k1", "v1"),
    Redis:get("k1"),
    redis_client:stop(Redis).

e.g. 2 (with registered name)
    Name = redis_client:name(Host, Port),
    {ok, Pid} = redis_client:start(Host, Port, "passwd", Name),
    Redis = redics_client:handler(Name),
    Redis:set("k1", "v1"),
    Redis:get("k1"),
    redis_client:stop(Redis).

e.g. 3 (use the redis in OTP)
    % in main supervisor:
    {redis_client_sup, {redis_conn_sup, start_link, []},
        permanent, 1000, supervisor, [redis_client]}

    % start a redis client:
    Name = redis_client:name(Host, Port),
    {ok, _} = redis_cilent_sup:connect(Host, Port, Pass, Name),
    Redis = redis_client:handler(Name),
    Redis:set("k1", "v1"),
    Redis:get("k1").

e.g. 4 (use in OTP with connection pool)
    % in main supervisor:
    {redis_client_sup, {redis_conn_sup, start_link, []},
        permanent, 1000, supervisor, [redis_client]}

    % start client pool
    [begin
        Name = redis_client:name(Host, Port, I)
        {ok, _} = redis_cilent_sup:connect(Host, Port, Pass, Name)
    end || I <- lists:seq(1, 5)],

    % random select a client
    Selected = redis_client:existing_name(Host, Port, random:uniform(5)),
    Redis = redis_client:handler(Selected),
    Redis:set("k1", "v1"),
    Redis:get("k1").

## Benchmark
in my laptop, 
CPU: Intel(R) Core(TM)2 Duo CPU T5870)
OS: ubuntu 10.04
redis_version:2.1.1
arch_bits:32
run the test/redis-benchmark, the benchmark result is:

    ./redis-benchmark -s localhost -n 10000 -c 10 -o 10
    usage:
     redis-benchmark [options]
      -s Server   - the redis server
      -p Port     - the redis server port (default 6379) 
      -c Clients  - the concurrent request client number (default 5) 
      -n Count    - the total requests count (default 10000) 
      -o Pool     - the connection pool size (default 5) 

    ===== "PING" ======
    10000 requests completed in 0.50 s
    20130 requests per second
    ===== "SET" ======
    10000 requests completed in 0.67 s
    14907 requests per second
    ===== "GET" ======
    10000 requests completed in 0.68 s
    14669 requests per second
    ===== "INCR" ======
    10000 requests completed in 0.65 s
    15444 requests per second
    ===== "LPUSH" ======
    10000 requests completed in 0.50 s
    20158 requests per second
    ===== "LPOP" ======
    10000 requests completed in 0.64 s
    15726 requests per second

## Version
the erl-redis version is synchronous with redis server version. 
Subtracting one from the Redis server major version will get the erl-redis version.
e.g. if you want to request the Redis 1.2.6, you need the erl-redis with version 0.2.6
