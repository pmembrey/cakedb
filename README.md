cakedb
======

CakeDB - Now you really can have your cake and eat it!

###PropEr testing

In order to do so you will need to first build a release of CakeDB with:

`rebar generate`

You can then start a console in the new release with:

`rel/cake/bin/cake console_clean`

Once the console is ready you can run the tests with:


`> proper:quickcheck(cake_streams_statem:prop_cake_streams_work(),[{numtests,100}]). `

\* If you don't have a release ready, you can build one with `rebar get-deps`, `rebar compile` and finally `rebar generate`.
