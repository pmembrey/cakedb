-module(cake_streams_statem).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-export([initial_state/0, command/1, precondition/2, postcondition/3,
        next_state/3, cleanup/1]).

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
    oneof([
           {call, ?SERVER, register_stream, [streamname()]},
           {call, ?SERVER, stream_filename, [streamid(S)]},
           {call, gproc, send, [{n,l,{stream,streamid(S)}},binary()]},
           {call, gproc, send, [{n,l,{stream,streamid(S)}},clear]}
          ]).

% define when a command is valid
precondition(S, {call,gproc,send, [{n,l,{stream,StreamID}},_Msg]}) ->
    proplists:is_defined(StreamID, S#state.streams);
precondition(_S, _command) ->
    true.

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
% All the other commands do not change the abstract state
next_state(S, _V, _Command) ->
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
    end;
postcondition(_S, {call, gproc, send, [{n,l,{stream,_StreamID}}, Msg]}, Result) ->
    Result =:= Msg.

%%-----------------------------------------------------------------------------
%% Properties
%%-----------------------------------------------------------------------------

prop_cake_streams_work() ->
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

% Return any of the existing StreamID
% plus an unassgined one for testing
streamid(#state{counter = Counter}) ->
    elements(lists:seq(1, Counter+1)).

%%-----------------------------------------------------------------------------
%% utils
%%-----------------------------------------------------------------------------

cleanup(StreamName) ->
    case application:get_env(cake,data_dir) of 
        {ok,DataDir} -> DirName = DataDir++StreamName++"/",
                        file:delete(DirName++StreamName++".index"),
                        file:delete(DirName++StreamName++".data"),
                        file:del_dir(DirName);
        undefined ->    lager:warning("Couldn't delete CakeDB data directory: Couldn't find CakeDB data directory from config file...")
    end,
    ok.
