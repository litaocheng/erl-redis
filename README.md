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

    the redis library has two kinds of modes:
    standalone mode:
        you can use redis module to everything. first you must setup an connection
        with redis server, then you can call commands and get reply from the server.
    
    application mode:
        this mode is working like the standard erlang library. e.g. sasl, crypto, inets.
        you must call application:start(redis) when you want to use redis client.
        the erl redis application will manager the connections with redis server automatically.

    if you want do some trivial work you had better use standalone mode, it is simple enough.
    if you want use redis in production, you had better use application mode. it is so robust
    by use erlang OTP.
