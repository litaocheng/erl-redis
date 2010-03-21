# README
erl-redis is redis client library for erlang.
you can get much more information about redis at http://github.com/antirez/redis.

## Install

    $ make test
    $ make
    $ make install

the make install statement will install the redis library into the 
$ERL_TOP/lib/ directory (kernel, stdlib also in that directory).
now you can use the redis client in your application.

## Usage

the redis application is working like the standard erlang library. e.g. sasl, crypto, inets.
you must call application:start(redis) when you want to use redis client.
the erl redis application will manager the connections with redis server automatically.

single server:
    redis_app:start(), % application:start(redis)
    ok = redis:single_server(localhost, 6379),
    % or ok = redis:single_server(localhost, 6379, 5),  % connection pool is 5 
    % or ok = redis:single_server(localhost, 6379, 5, "pwd"), % connection pool is 5, passwd is "pwd"
    redis:exists("key"). 

consistent servers:
    redis_app:start(),
    Servers = [{localhost, 6379, 3}, {localhost, 6380, 5}],
    ok = redis:multi_servers(Servers).
    % or ok = redis:multi_servers(Servers, "pwd"). % passwd is "pwd"
    redis:exists("key").
