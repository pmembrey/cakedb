-module(cakedb_driver_statem).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-export([initial_state/0, command/1, precondition/2,
        postcondition/3, next_state/3, diff/2, sum/2, timestamp/0]).

-define(DRIVER, cakedb_driver).
-define(TIMEOUT, 1000). % milliseconds
-define(STREAMNAMES, ["tempfile", "file001", "anotherfile",
        "somefile", "binfile", "cakestream"]).

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
    frequency(
        [
            {9,{call,?DRIVER,append,[elements(?STREAMNAMES), list(integer(65,122))]}},
            {1,symb_call(range_query, elements(?STREAMNAMES), S#state.starttime)},
            {1,symb_call(all_since, elements(?STREAMNAMES), S#state.starttime)},
            {1,symb_call(last_entry_at, elements(?STREAMNAMES), S#state.starttime)}
        ]
    ).

% define when a command is valid
precondition(_S,{call,?DRIVER,append,[_StreamName, _Data]}) ->
    true;
precondition(S,{call,?DRIVER,_Query,[StreamName|_OtherArgs]}) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,Data}} ->
            case Data of
                [] ->
                    true;
                _ ->
                    % Make sure CakeDB had time to flush
                    timer:sleep(?TIMEOUT),
                    true
            end;
        false ->
            true
    end.

% define the state transitions triggered by each command
next_state(S,_V,{call,?DRIVER,append,[StreamName,Data]}) ->
    case Data of
        [] ->
            S;
        _ ->
            case lists:keysearch(StreamName,1,S#state.streams) of
                {value, {StreamName,OldData}} ->
                    NewTuple = {StreamName,[{timestamp(),Data}|OldData]},
                    S#state{
                        streams = lists:keyreplace(StreamName,1,S#state.streams,NewTuple)
                    };
                false ->
                    NewTuple = {StreamName,[{timestamp(),Data}]},
                    S#state{streams = [NewTuple|S#state.streams]}
            end
    end;
next_state(S,_V,{call,?DRIVER,_Query,[StreamName|_OtherArgs]}) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,_Data}} ->
            S;
        false ->
            S#state{streams = [{StreamName,[]}|S#state.streams]}
    end.

% define the conditions needed to be
% met in order for a test to pass
postcondition(S, {call,?DRIVER,range_query,[StreamName,Start,End]}, Result) ->
    case Start > End of
        true ->
            Expected = [];
        false ->
            case lists:keysearch(StreamName,1,S#state.streams) of
                {value, {StreamName,Data}} ->
                    Expected = lists:reverse([Y || {X,Y} <- Data, X>=Start, X=<End]);
                false ->
                    Expected = []
            end
    end,
    Observed = [Y || {_,Y} <- Result],
    Expected =:= Observed;
postcondition(S, {call,?DRIVER,all_since,[StreamName,TS]}, Result) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,Data}} ->
            Expected = lists:reverse([Y || {X,Y} <- Data, X>=TS]);
        false ->
            Expected = []
    end,
    Observed = [Y || {_,Y} <- Result],
    Expected =:= Observed;
postcondition(S, {call,?DRIVER,last_entry_at,[StreamName,TS]}, Result) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,Data}} ->
            Smaller = [{X,Y} || {X,Y} <- Data, X=<TS],
            case Smaller of
                [] ->
                    true;
                _ ->
                    {_,Expected} = lists:last(lists:keysort(1,Smaller)),
                    [{_,Observed}] = Result,
                    Expected =:= Observed
            end;
        false ->
            true
    end;
postcondition(_S, _V, _Result) ->
    true.

%%-----------------------------------------------------------------------------
%% properties
%%-----------------------------------------------------------------------------

prop_cakedb_driver_works() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            ?DRIVER:start_link("localhost",8888),
            {History,State,Result} = run_commands(?MODULE, Cmds),
            ?DRIVER:stop(),
            ?WHENFAIL(
                io:format("\n\nHistory: ~w\n\nState: ~w\n\nResult: ~w\n\n",
                [History,State,Result]),
                aggregate(command_names(Cmds), Result =:= ok))
        end).

%%-----------------------------------------------------------------------------
%% utils
%%-----------------------------------------------------------------------------

% Returns a symbolic call to one of the driver's functions
symb_call(Function, StreamName, MinTime) ->
    Now = {call, ?MODULE, timestamp, []},
    Diff = {call, ?MODULE, diff, [Now, MinTime]},
    Rand = {call, random, uniform, [Diff]},
    TS = {call, ?MODULE, sum, [MinTime, Rand]},
    case Function of
        range_query ->
            Diff2 = {call, ?MODULE, diff, [Now, TS]},
            Rand2 = {call, random, uniform, [Diff2]},
            TS2 = {call, ?MODULE, sum, [TS, Rand2]},
            {call, ?DRIVER, range_query, [StreamName, TS, TS2]};
        _ -> % all_since and last_entry_at
            {call, ?DRIVER, Function, [StreamName, TS]}
    end.

diff(A,B) ->
    A-B.

sum(A,B) ->
    A+B.

% Returns the current timestamp
timestamp() ->
    {Mega, Sec, Micro} = now(),
    Mega * 1000000 * 1000000 + Sec * 1000000 + Micro.

