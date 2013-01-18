-module(cakedb_driver_tests).

-include_lib("eunit/include/eunit.hrl").

-define(TIMEOUT,12000). % time delay in ms to let CakeDB flush

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FIXTURES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start() ->
    io:format(user,"Initiating tests...~n",[]),
    StartTime = timestamp(),
    {ok, Pid} = cakedb_driver:start_link("localhost",8888),
    {Pid,StartTime}.
 
stop({Pid,_StartTime}) ->
    io:format(user,"Cleaning up...~n",[]),
    cakedb_driver:stop(),
    exit(Pid, ok).
 
instantiator({_Pid,TimeStamp0}) ->
    StreamName = "Erlang_Driver_Test_Stream",
    Query1 = cakedb_driver:all_since(StreamName,TimeStamp0),
    Query2 = cakedb_driver:range_query(StreamName,TimeStamp0,timestamp()),
    io:format(user,"Sending first batch of data to CakeDB...~n",[]),
    cakedb_driver:append(StreamName,"HelloWorld"),
    cakedb_driver:append(StreamName,"SomeData"),
    cakedb_driver:append(StreamName,"SomeMoreData"),
    io:format(user,"Giving ~p seconds to CakeDB to flush...~n",[?TIMEOUT/1000]),
    timer:sleep(?TIMEOUT), % Give time to CakeDB to flush
    TimeStamp1 = timestamp(),
    Query3 = cakedb_driver:all_since(StreamName,TimeStamp0),
    io:format(user,"Sending second batch of data to CakeDB...~n",[]),
    cakedb_driver:append(StreamName,"~!@#$%^&*()_+"),
    cakedb_driver:append(StreamName,"0123456789"),
    cakedb_driver:append(StreamName,"<<0011>>,<<\"aaAA\">>"),
    io:format(user,"Giving ~p seconds to CakeDB to flush...~n",[?TIMEOUT/1000]),
    timer:sleep(?TIMEOUT), % Give time to CakeDB to flush
    io:format(user,"Running tests...~n",[]),
    TimeStamp2 = timestamp(),
    Query4 = cakedb_driver:range_query(StreamName,TimeStamp0,TimeStamp1),
    Query5 = cakedb_driver:last_entry_at(StreamName,TimeStamp1),
    Query6 = cakedb_driver:all_since(StreamName,TimeStamp1),
    Query7 = cakedb_driver:range_query(StreamName,TimeStamp1,TimeStamp2),
    Query8 = cakedb_driver:last_entry_at(StreamName,TimeStamp2),
    [
        ?_assertMatch([],Query1),
        ?_assertMatch([],Query2),
        ?_assertMatch([{_,_},{_,_},{_,_}],Query3),
        ?_assertEqual(element(2,lists:nth(1,Query3)),"HelloWorld"),
        ?_assertEqual(element(2,lists:nth(2,Query3)),"SomeData"),
        ?_assertEqual(element(2,lists:nth(3,Query3)),"SomeMoreData"),
        ?_assertMatch([{_,_},{_,_},{_,_}],Query4),
        ?_assertEqual(element(2,lists:nth(1,Query4)),"HelloWorld"),
        ?_assertEqual(element(2,lists:nth(2,Query4)),"SomeData"),
        ?_assertEqual(element(2,lists:nth(3,Query4)),"SomeMoreData"),
        ?_assertMatch([{_,_}],Query5),
        ?_assertEqual(element(2,lists:nth(1,Query5)),"SomeMoreData"),
        ?_assertMatch([{_,_},{_,_},{_,_}],Query6),
        ?_assertEqual(element(2,lists:nth(1,Query6)),"~!@#$%^&*()_+"),
        ?_assertEqual(element(2,lists:nth(2,Query6)),"0123456789"),
        ?_assertEqual(element(2,lists:nth(3,Query6)),"<<0011>>,<<\"aaAA\">>"),
        ?_assertMatch([{_,_},{_,_},{_,_}],Query7),
        ?_assertEqual(element(2,lists:nth(1,Query7)),"~!@#$%^&*()_+"),
        ?_assertEqual(element(2,lists:nth(2,Query7)),"0123456789"),
        ?_assertEqual(element(2,lists:nth(3,Query7)),"<<0011>>,<<\"aaAA\">>"),
        ?_assertMatch([{_,_}],Query8),
        ?_assertEqual(element(2,lists:nth(1,Query8)),"<<0011>>,<<\"aaAA\">>")
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

