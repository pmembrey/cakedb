CakeDB Erlang Driver
======

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

1. Compile the cakedb_driver and cakedb_driver_tests modules:

`> c(cakedb_driver).`

and

`> c(cakedb_driver_tests).`

2. Insure an instance of CakeDB is running on
host = "localhost" and port 8888.

3. Then run:

`> eunit:test(cakedb_driver).`

in the Erlang shell.

