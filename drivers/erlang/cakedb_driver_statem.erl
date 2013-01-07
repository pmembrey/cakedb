-module(cakedb_driver_statem).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-export([initial_state/0, command/1, precondition/2, postcondition/3,
        next_state/3,request_stream/1,request_stream_with_size/2,
        append/2,simple_query/3,all_since/2,last_entry_at/2,
        timestamp/0,payload_to_list/2]).

-define(STREAMNAMES, [<<"tempfile">>, <<"file001">>, <<"anotherfile">>,
        <<"somefile">>, <<"binfile">>, <<"cakestream">>]).

% E.g. of model state structure:
% {
%   starttime = 13554171361963445,
%   counter = N,
%   streams = [
%     {StreamName_1, [{TS_1,<<"DATA_1">>}, ..., {TS_M,<<"DATA_M">>}]},
%     ...,
%     {StreamName_N, [{TS_1,<<"DATA_1">>}, ..., {TS_L,<<"DATA_L">>}]}
%   ]
% }
-record(state,{starttime,counter,streams}).

%%-----------------------------------------------------------------------------
%% statem callbacks
%%-----------------------------------------------------------------------------

% initialize the state machine
initial_state() ->
    #state{
            starttime = timestamp(),
            counter = 0,
            streams = []
          }.

% define the commands to test
command(S) ->
    oneof([
        {call,cakedb_driver,append,[elements(?STREAMNAMES),list(integer(32,255))]},
        {call,cakedb_driver,range_query,[
                elements(?STREAMNAMES),
                integer(S#state.starttime,timestamp_wrapper()),
                integer(S#state.starttime,timestamp_wrapper())]},
        {call,cakedb_driver,all_since,[elements(?STREAMNAMES),integer(S#state.starttime,timestamp_wrapper())]},
        {call,cakedb_driver,last_entry_at,[elements(?STREAMNAMES),integer(S#state.starttime,timestamp_wrapper())]}
    ]).

% define when a command is valid
precondition(S,{call,cakedb_driver,last_entry_at,[StreamName,_Time]}) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,Data}} ->
            Data =/= [];
        false ->
            false
    end;
precondition(_S, _Command) ->
    true.

% define the state transitions triggered by each command
next_state(S,_V,{call,cakedb_driver,append,[StreamName,Data]}) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,OldData}} ->
            NewTuple = {StreamName,[{timestamp(),Data}|OldData]},
            S#state{
                streams = lists:keyreplace(StreamName,1,S#state.streams,NewTuple)
            };
        false ->
            NewTuple = {StreamName,[{timestamp(),Data}]},
            S#state{streams = [NewTuple|S#state.streams]}
    end;
next_state(S,_V,{call,cakedb_driver,range_query,[StreamName,_Start,_End]}) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,_Data}} ->
            S;
        false ->
            S#state{streams = [{StreamName,[]}|S#state.streams]}
    end;
next_state(S,_V,{call,cakedb_driver,all_since,[StreamName,_Time]}) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,_Data}} ->
            S;
        false ->
            S#state{streams = [{StreamName,[]}|S#state.streams]}
    end;
next_state(S,_V,{call,cakedb_driver,last_entry_at,[StreamName,_Time]}) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,_Data}} ->
            S;
        false ->
            S#state{streams = [{StreamName,[]}|S#state.streams]}
    end.

% define the conditions needed to be
% met in order for a test to pass
postcondition(S, {call,cakedb_driver,range_query,[StreamName,Start,End]}, Result) ->
    case Start > End of
        true ->
            Result =:= [];
        false ->
            case lists:keysearch(StreamName,1,S#state.streams) of
                {value, {StreamName,Data}} ->
                    Expected = lists:sort([Y || {X,Y} <- Data, X>=Start, X=<End]),
                    Observed = lists:sort([Y || {X,Y} <- Result, X>=Start, X=<End]),
                    Observed =:= Expected;
                false ->
                    Result =:= []
            end
    end;
postcondition(S, {call,cakedb_driver,all_since,[StreamName,Time]}, Result) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,Data}} ->
            Expected = lists:sort([Y || {X,Y} <- Data, X>=Time]),
            Observed = lists:sort([Y || {X,Y} <- Result, X>=Start, X=<End]),
            Observed =:= Expected;
        false ->
            Result =:= []
    end;
postcondition(S, {call,cakedb_driver,last_entry_at,[StreamName,Time]}, Result) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,Data}} ->
            Expected = element(2,lists:max([{Y,X} || {Y,X} <- Data, Y=<Time])),
            Observed = element(2,lists:nth(1,Result)),
            Observed =:= Expected;
        false ->
            Result =:= []
    end;
postcondition(_S, _V, _Result) ->
    true.

%%-----------------------------------------------------------------------------
%% properties
%%-----------------------------------------------------------------------------

prop_cakedb_driver_works() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            cakedb_driver:start_link(),
            {History,State,Result} = run_commands(?MODULE, Cmds),
            cakedb_driver:stop(),
            ?WHENFAIL(
                io:format("\n\nHistory: ~w\n\nState: ~w\n\nResult: ~w\n\n",
                [History,State,Result]),
                aggregate(command_names(Cmds), Result =:= ok))
        end).

%%-----------------------------------------------------------------------------
%% utils
%%-----------------------------------------------------------------------------

% Returns the current timestamp
timestamp() ->
    {Mega, Sec, Micro} = now(),
    Mega * 1000000 * 1000000 + Sec * 1000000 + Micro.

% Returns symbolic call to timestamp/0 function
timestamp_wrapper() ->
    {call,?MODULE,timestamp,[]}.

