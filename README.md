cakedb
======

CakeDB - Now you really can have your cake and eat it!

###PropEr testing

Selected modules can be tested using the [PropEr](http://proper.softlab.ntua.gr/) test suite. In order to do so, fire up the Erlang shell from the main directory (cakedb/) with the necessary paths included*:

`$ erl -pa deps/*/ebin -pa ebin`

and run the tests commands:

`> proper:quickcheck(cake_stream_manager_statem:prop_cake_stream_manager_works(),[{numtests,100}]). `

\* You might have to run `$ rebar get-deps` and `$ rebar compile ` first if you have not do so already.
