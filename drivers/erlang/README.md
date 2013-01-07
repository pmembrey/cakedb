CakeDB Erlang Driver
======

###API

`cakedb_driver:start_link() ->
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

###PropEr testing

In order to do so you will need to first build a release of CakeDB with:

`rebar generate`

You can then start a console in the new release with:

`rel/cake/bin/cake console_clean`

Once the console is ready you can run the tests with:

`> proper:quickcheck(cakedb_driver_statem:prop_cakedb_driver_works(),[{numtests,100}]). `

The second set of tests may take a while to run as a time buffer is spent to insure the database flushes between writes and reads.

\* If you don't have a release ready, you can build one with `rebar get-deps`, `rebar compile` and finally `rebar generate`.

