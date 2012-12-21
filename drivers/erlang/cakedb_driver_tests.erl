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
    [{_TS1,Data1},{_TS2,Data2},{_TS3,Data3}] = Range_query,
    All_since = cakedb_driver:all_since(StreamName,StartTime),
    [{_TS4,Data4},{_TS5,Data5},{_TS6,Data6}] = All_since,
    Last_entry_at = cakedb_driver:last_entry_at(StreamName,timestamp()),
    [{_TS7,Data7}] = Last_entry_at,
    [
        ?_assertEqual(Data1,"HelloWorld"),
        ?_assertEqual(Data2,"SomeData"),
        ?_assertEqual(Data3,"SomeMoreData"),
        ?_assertEqual(Data4,"HelloWorld"),
        ?_assertEqual(Data5,"SomeData"),
        ?_assertEqual(Data6,"SomeMoreData"),
        ?_assertEqual(Data7,"SomeMoreData")
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

