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
