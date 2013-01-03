-module(cakedb_driver_tests).

-include_lib("eunit/include/eunit.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FIXTURES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start() ->
    StreamName = "Erlang_Driver_Test_Stream",
    StartTime = timestamp(),
    {ok, Pid} = cakedb_driver:start_link(),
    cakedb_driver:append(StreamName,"HelloWorld"),
    cakedb_driver:append(StreamName,"SomeData"),
    cakedb_driver:append(StreamName,"SomeMoreData"),
    timer:sleep(10000), % Give time to CakeDB to flush
    {Pid,StreamName,StartTime}.
 
stop({Pid,_StreamName,_StartTime}) ->
    cakedb_driver:stop(),
    exit(Pid, ok).
 
instantiator({_Pid,StreamName,StartTime}) ->
    Range_query = cakedb_driver:range_query(StreamName,StartTime,timestamp()),
    All_since = cakedb_driver:all_since(StreamName,StartTime),
    Last_entry_at = cakedb_driver:last_entry_at(StreamName,timestamp()),
    [
        ?_assertMatch([{_,_},{_,_},{_,_}],Range_query),
        ?_assertEqual(element(2,lists:nth(1,Range_query)),"HelloWorld"),
        ?_assertEqual(element(2,lists:nth(2,Range_query)),"SomeData"),
        ?_assertEqual(element(2,lists:nth(3,Range_query)),"SomeMoreData"),
        ?_assertMatch([{_,_},{_,_},{_,_}],All_since),
        ?_assertEqual(element(2,lists:nth(1,All_since)),"HelloWorld"),
        ?_assertEqual(element(2,lists:nth(2,All_since)),"SomeData"),
        ?_assertEqual(element(2,lists:nth(3,All_since)),"SomeMoreData"),
        ?_assertMatch([{_,_}],Last_entry_at),
        ?_assertEqual(element(2,lists:nth(1,Last_entry_at)),"SomeMoreData")
    ].
        
cakedb_driver_test_() ->
    {setup,
     fun start/0,
     fun stop/1,
     fun instantiator/1}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% UTILS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

timestamp() ->
    {Mega, Sec, Micro} = now(),
    Mega * 1000000 * 1000000 + Sec * 1000000 + Micro.

