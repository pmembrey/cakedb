-module(cake_stream_manager_statem).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-export([initial_state/0, command/1, precondition/2, postcondition/3,
        next_state/3]).

-record(state,{streams,counter}).

-define(SERVER, cake_stream_manager).
-define(APP, cake_app).
-define(STREAMNAMES, [<<"tempfile">>, <<"file001">>, <<"anotherfile">>,
        <<"somefile">>, <<"binfile">>, <<"cakestream">>]).

%%-----------------------------------------------------------------------------
%% Statem callbacks
%%-----------------------------------------------------------------------------

initial_state() ->
    #state{streams = [], counter = 0}.

command(S) ->
    oneof([{call, ?SERVER, register_stream, [streamname()]},
           {call, ?SERVER, stream_filename, [streamid(S)]}]).

precondition(_S, _command) ->
    true. % All preconditions are valid

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
            io:format("FileName: ~p\n",[FileName]),
            io:format("Result: ~p\n\n",[Result]),
            Result =:= FileName;
        false ->
            Result =:= unregistered_stream
    end.

%%-----------------------------------------------------------------------------
%% Properties
%%-----------------------------------------------------------------------------

prop_cake_stream_manager_works() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {ok, Pid} = ?APP:start([],[]),
                {History,State,Result} = run_commands(?MODULE, Cmds),
                ?APP:stop([]),
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

