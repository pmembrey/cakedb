-module(cake_streams_statem).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-export([initial_state/0, command/1, precondition/2, postcondition/3,
        next_state/3]).

-record(state,{starttime,counter,streams}).

-define(SERVER, cake_stream_manager).
-define(APP, cake).

%%-----------------------------------------------------------------------------
%% Statem callbacks
%%-----------------------------------------------------------------------------

% initialize the state machine
initial_state() ->
    #state{streams = [], counter = 0}.

% define the commands to test
command(S) ->
    oneof([
           {call, ?SERVER, register_stream, [proper_utils:streamname()]},
           {call, ?SERVER, stream_filename, [proper_utils:streamid(S)]},
           {call, gproc, send, [{n,l,{stream,proper_utils:streamid(S)}},binary()]},
           {call, gproc, send, [{n,l,{stream,proper_utils:streamid(S)}},clear]}
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
                [proper_utils:cleanup(X) || {_,{_,X}} <- State#state.streams],
                ?WHENFAIL(
                    io:format("\n\nHistory: ~w\n\nState: ~w\n\nResult: ~w\n\n",
                    [History,State,Result]),
                aggregate(command_names(Cmds), Result =:= ok))
            end).

