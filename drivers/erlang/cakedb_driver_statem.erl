-module(cakedb_driver_statem).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-export([initial_state/0, command/1, precondition/2,
        postcondition/3, next_state/3, diff/2, sum/2, timestamp/0]).

-define(DRIVER, cakedb_driver).

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
    oneof([
        symb_call(append, elements(?STREAMNAMES), null),
%        symb_call(range_query, elements(?STREAMNAMES), S#state.starttime),
        symb_call(all_since, elements(?STREAMNAMES), S#state.starttime)%,
%        symb_call(last_entry_at, elements(?STREAMNAMES), S#state.starttime)
    ]).

% define when a command is valid
precondition(S,{call,?DRIVER,last_entry_at,[StreamName,TS]}) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,Data}} ->
            case Data of
                [] ->
                    false;
                _ ->
                    TimeStamps = [X || {X,_} <- Data],
                    TS > lists:min(TimeStamps)
            end;
        false ->
            false
    end;
precondition(_S, _Command) ->
    true.

% define the state transitions triggered by each command
next_state(S,_V,{call,?DRIVER,append,[StreamName,Data]}) ->
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
next_state(S,_V,{call,?DRIVER,range_query,[StreamName,_Start,_End]}) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,_Data}} ->
            S;
        false ->
            S#state{streams = [{StreamName,[]}|S#state.streams]}
    end;
next_state(S,_V,{call,?DRIVER,all_since,[StreamName,_TS]}) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,_Data}} ->
            S;
        false ->
            S#state{streams = [{StreamName,[]}|S#state.streams]}
    end;
next_state(S,_V,{call,?DRIVER,last_entry_at,[StreamName,_TS]}) ->
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
            Result =:= [];
        false ->
            case lists:keysearch(StreamName,1,S#state.streams) of
                {value, {StreamName,Data}} ->
                    Expected = lists:sort([Y || {X,Y} <- Data, X>=Start, X=<End]),
                    Observed = lists:sort([Y || {_,Y} <- Result]),
                    Observed =:= Expected;
                false ->
                    Result =:= []
            end
    end;
postcondition(S, {call,?DRIVER,all_since,[StreamName,TS]}, Result) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,Data}} ->
            Expected = [Y || {X,Y} <- Data, X>=TS];
        false ->
            Expected = []
    end,
    Observed = [Y || {_,Y} <- Result],
    is_subset(Expected, Observed);
postcondition(S, {call,?DRIVER,last_entry_at,[StreamName,TS]}, Result) ->
    case lists:keysearch(StreamName,1,S#state.streams) of
        {value, {StreamName,Data}} ->
            io:format("~n[Y || {X,Y} <- Data]: ~p~n",[[Y || {X,Y} <- Data]]),%, X=<TS]]),
            io:format("Result: ~p~n~n",[Result]),
            true;
%            lists:member(element(2,lists:nth(1,Result)), [Y || {X,Y} <- Data, X=<TS]);
        false ->
            io:format("~nResult (Should be empty): ~p~n",[Result]),
%            Result =:= []
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
            ?DRIVER:start_link(),
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
    timer:sleep(50), % give time to CakeDB to flush
    case Function of
        append ->
            {call, ?DRIVER, append, [StreamName, list(integer(65,122))]};
        range_query ->
            Start = integer(MinTime, timestamp()),
            End = integer(MinTime, timestamp()),
            {call, ?DRIVER, range_query, [StreamName, Start, End]};
        _ -> % all_since and last_entry_at
            Now = {call, ?MODULE, timestamp, []},
            Diff = {call, ?MODULE, diff, [Now, MinTime]},
            Rand = {call, random, uniform, [Diff]},
            TS = {call, ?MODULE, sum, [MinTime, Rand]},
            {call, ?DRIVER, Function, [StreamName, TS]}
    end.

diff(A,B) ->
    A-B.

sum(A,B) ->
    A+B.

% Check if list A is subset of list B
is_subset(A,B) ->
    lists:all(fun(X) -> lists:member(X,B) end, A).

% Returns the current timestamp
timestamp() ->
    {Mega, Sec, Micro} = now(),
    Mega * 1000000 * 1000000 + Sec * 1000000 + Micro.

