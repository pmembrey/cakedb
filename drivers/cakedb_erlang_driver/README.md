CakeDB Erlang Driver
======

###Getting Started

CakeDB's Erlang driver uses [rebar](https://github.com/basho/rebar) as
build tool. Once installed, you can deploy the driver using the following
commands:

    $ cd /path/to/cakedb_erlang_driver/
    $ rebar get-deps
    $ rebar compile

The cakedb_driver module with the API described below can then be used
Erlang code.

###API

`cakedb_driver:start_link(Host,Port) ->
        {ok, Pid}`

`cakedb_driver:append(StreamName,Data) ->
        ok`

`cakedb_driver:range_query(StreamName,Start,End) ->
        [{TS_1,DATA_1},...,{TS_N,DATA_N}]`

`cakedb_driver:all_since(StreamName,Time) ->
        [{TS_1,DATA_1},...,{TS_N,DATA_N}]`

`cakedb_driver:last_entry_at(StreamName,Time) ->
        [{TS,DATA}]`

`cakedb_driver:stop() ->
        normal`

###EUnit testing

To run unit tests on CakeDB's Erlang driver:

1. Insure an instance of CakeDB is running on
host = "localhost" and port 8888.

2. Then run:

`> eunit:test(cakedb_driver).`

in the Erlang shell.

###PropEr testing

To run PropEr tests:

1. In one terminal, launch CakeDB with environment variable
write_delay overwritten to zero:

        $ cd /path/to/cakedb/ebin/
        $ erl -pa ../deps/*/ebin -cake write_delay 0
        > application:start(cake).

2. In another terminal, launch Erlang from the Erlang driver
directory:
```
$ cd /path/to/cakedb_erlang_driver/ebin/
$ erl -pa ../deps/*/ebin/
```
3. Run the command:
```
> proper:quickcheck(cakedb_driver_statem:prop_cakedb_driver_works(),[{numtests,20}]).
```

