-module(cake_stream_manager_statem).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-export([initial_state/0, command/1, precondition/2, postcondition/3,
        next_state/3]).

-record(state,{streams,counter}).

-define(SERVER, cake_stream_manager).
-define(APP, cake).
-define(STREAMNAMES, [<<"tempfile">>, <<"file001">>, <<"anotherfile">>,
        <<"somefile">>, <<"binfile">>, <<"cakestream">>]).

%%-----------------------------------------------------------------------------
%% Statem callbacks
%%-----------------------------------------------------------------------------

% initialize the state machine
initial_state() ->
    #state{streams = [], counter = 0}.

% define the commands to test
command(S) ->
    oneof([{call, ?SERVER, register_stream, [streamname()]},
           {call, ?SERVER, stream_filename, [streamid(S)]}]).

% define when a command is valid
precondition(_S, _command) ->
    true. % All preconditions are valid

% define the state transitions triggered
% by each command
next_state(S, _V, {call, ?SERVER, register_stream, [Stream]}) ->
    case proplists:is_defined(Stream, S#state.streams) of
        true ->
            S;
        false ->
            StreamID = S#state.counter+1,
            FileName = binary_to_list(Stream),
            S#state{
                counter=StreamID,
                streams=lists:flatten([
                        [
                            {Stream,{StreamID,FileName}},
                            {StreamID,{Stream,FileName}}
                        ]
                    |S#state.streams])}
    end;
next_state(S, _V, {call, ?SERVER, stream_filename, [_StreamID]}) ->
    S.

% define the conditions needed to be
% met in order for a test to pass
postcondition(S, {call, ?SERVER, register_stream, [StreamName]}, Result) ->
    case proplists:is_defined(StreamName,S#state.streams) of
        true ->
            {StreamID,_FileName} =
                proplists:get_value(StreamName,S#state.streams),
            Result =:= StreamID;
        false ->
            Result =:= S#state.counter + 1
    end;
postcondition(S, {call, ?SERVER, stream_filename, [StreamID]}, Result) ->
    case proplists:is_defined(StreamID,S#state.streams) of
        true ->
            {_StreamName,FileName} =
                proplists:get_value(StreamID,S#state.streams),
            Result =:= {ok,FileName};
        false ->
            Result =:= unregistered_stream
    end.

%%-----------------------------------------------------------------------------
%% Properties
%%-----------------------------------------------------------------------------

prop_cake_stream_manager_works() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                application:start(?APP),
                {History,State,Result} = run_commands(?MODULE, Cmds),
                application:stop(?APP),
                [cleanup(X) || {_,{_,X}} <- element(2,State)],
                ?WHENFAIL(
                    io:format("\n\nHistory: ~w\n\nState: ~w\n\nResult: ~w\n\n",
                    [History,State,Result]),
                aggregate(command_names(Cmds), Result =:= ok))
            end).

%%-----------------------------------------------------------------------------
%% generators
%%-----------------------------------------------------------------------------

streamname() -> 
    elements(?STREAMNAMES).

streamid(#state{counter = Counter}) ->
    % for testing purpose, return any integer
    % between zero and twice the counter plus one.
    elements(lists:seq(0, 2*Counter+1)).

%%-----------------------------------------------------------------------------
%% utils
%%-----------------------------------------------------------------------------

cleanup(StreamName) ->
    DirName = "data/cakedb_data/"++StreamName++"/",
    file:delete(DirName++StreamName++".index"),
    file:delete(DirName++StreamName++".data"),
    file:del_dir(DirName),
    ok.
