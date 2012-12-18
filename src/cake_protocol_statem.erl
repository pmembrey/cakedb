-module(cake_protocol_statem).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-export([initial_state/0, command/1, precondition/2, postcondition/3,
        next_state/3,request_stream/1,request_stream_with_size/2,
        append/2,simple_query/3,all_since/2,last_entry_at/2,
        timestamp/0,payload_to_list/2]).

% E.g. of model state structure:
% {
%   starttime = 13554171361963445,
%   counter = N,
%   streams = [
%     {StreamID_1, StreamName_1, [{TS_1,<<"DATA_1">>}, ..., {TS_M,<<"DATA_M">>}]},
%     ...,
%     {StreamID_N, StreamName_N, [{TS_1,<<"DATA_1">>}, ..., {TS_L,<<"DATA_L">>}]}
%   ]
% }
-record(state,{starttime,counter,streams}).

% Options
-define(NOOP, 0).
-define(REQUEST_STREAM_WITH_SIZE, 1).
-define(APPEND, 2).
-define(QUERY, 3).
-define(ALL_SINCE, 4).
-define(REQUEST_STREAM, 5).
-define(LAST_ENTRY_AT, 6).

% socket
-define(HOST,"localhost").
-define(PORT,8888).
-define(TIMEOUT,15000). % time delay in ms to let CakeDB flush

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
        {call,?MODULE,request_stream,[proper_utils:streamname()]},
        {call,?MODULE,request_stream_with_size,[pos_integer(),proper_utils:streamname()]},
        {call,?MODULE,append,[proper_utils:streamid(S),list(integer(32,255))]},
%        {call,?MODULE,simple_query,[proper_utils:streamid(S),S#state.starttime,timestamp_wrapper()]},
%        {call,?MODULE,all_since,[proper_utils:streamid(S),S#state.starttime]},
        {call,?MODULE,last_entry_at,[proper_utils:streamid(S),timestamp_wrapper()]}
    ]).

% define when a command is valid
precondition(S, {call,?MODULE,simple_query,[StreamID,_Start,_End]}) ->
    lists:keymember(StreamID,1,S#state.streams);
precondition(S, {call,?MODULE,all_since,[StreamID,_Timestamp]}) ->
    lists:keymember(StreamID,1,S#state.streams);
precondition(S, {call,?MODULE,last_entry_at,[StreamID,_Timestamp]}) ->
    case lists:keysearch(StreamID,1,S#state.streams) of
        {value, {StreamID,_StreamName,[]}} ->
            false;
        {value, {StreamID,_StreamName,_Data}} ->
            true;
        false ->
            false
    end;
precondition(_S, _Command) ->
    true.

% define the state transitions triggered
% by each command
next_state(S,{ok,<<_,ID>>},{call,?MODULE,request_stream_with_size,[_Size,StreamName]}) ->
    case lists:keymember(StreamName,2,S#state.streams) of
        false ->
            S#state{
                counter = S#state.counter + 1,
                streams = [{ID, StreamName, []}|S#state.streams]
            };
        true ->
            S
    end;
next_state(S,_V,{call,?MODULE,append,[StreamID,Data]}) ->
    case lists:keysearch(StreamID,1,S#state.streams) of
        {value, {StreamID,StreamName,OldData}} ->
            NewTuple = {StreamID,StreamName,[{timestamp(),Data}|OldData]},
            S#state{
                streams = lists:keyreplace(StreamID,1,S#state.streams,NewTuple)
            };
        false ->
            S
    end;
next_state(S,_V,{call,?MODULE,request_stream,[StreamName]}) ->
    case lists:keymember(StreamName,2,S#state.streams) of
        false ->
            S#state{
                counter = S#state.counter + 1,
                streams = [{S#state.counter +1, StreamName, []}|S#state.streams]
            };
        true ->
            S
    end;
% all the other commands do not change the abstract state
next_state(S, _V, _Command) ->
    S.

% define the conditions needed to be
% met in order for a test to pass
postcondition(S, {call,?MODULE,request_stream_with_size,[_Size,StreamName]}, Result) ->
    Stream = lists:keysearch(StreamName,2,S#state.streams),
    case {Stream,Result} of
        {{value,{StreamID,_StreamName,_Data}},ID} ->
            StreamID =:= ID;
        {false,_ID} ->
            true;
        _ ->
            false
    end;
postcondition(S, {call,?MODULE,append,[StreamID,_Data]}, Result) ->
    case lists:keymember(StreamID,1,S#state.streams) of
        true ->
            Result =:= casted;
        false ->
            Result =:= unregistered_stream
    end;
%postcondition(S, {call,?MODULE,simple_query,[StreamID,_Start,_End]}, Result) ->
%    case Result of
%        {ok, <<Payload/binary>>} ->
%            Stream = lists:keysearch(StreamID,1,S#state.streams),
%            case Stream of
%                {value,{_StreamID, _StreamName, Expected}} ->
%                    Observed = payload_to_list([],Payload),
%                    Expected_Sorted = lists:sort([X || {_,X} <- Expected]),
%                    Observed_Sorted = lists:sort([X || {_,X} <- Observed]),
%                    Expected_Sorted =:= Observed_Sorted;
%                _ ->
%                    false
%            end;
%        _ ->
%            false
%    end;
%postcondition(S, {call,?MODULE,all_since,[StreamID,Timestamp]}, Result) ->
%    case Result of
%        {ok, <<Payload/binary>>} ->
%            Stream = lists:keysearch(StreamID,1,S#state.streams),
%            case Stream of
%                {value,{_StreamID, _StreamName, Expected}} ->
%                    Observed = payload_to_list([],Payload),
%                    Expected_Sorted = lists:sort([X || {Y,X} <- Expected, Y>Timestamp]),
%                    Observed_Sorted = lists:sort([X || {_,X} <- Observed]),
%                    Expected_Sorted =:= Observed_Sorted;
%                _ ->
%                    false
%            end;
%        _ ->
%            false
%    end;
postcondition(S, {call,?MODULE,request_stream,[_Size,StreamName]}, Result) ->
    Stream = lists:keysearch(StreamName,2,S#state.streams),
    case {Stream,Result} of
        {{value,{StreamID,_StreamName,_Data}},ID} ->
            StreamID =:= ID;
        {false,_ID} ->
            true;
        _ ->
            false
    end;
postcondition(S, {call,?MODULE,last_entry_at,[StreamID,Timestamp]}, Result) ->
    case Result of
        {ok,<<Payload/binary>>} ->
            Stream = lists:keysearch(StreamID,1,S#state.streams),
            case Stream of
                {value,{_StreamID, _StreamName, Data}} ->
                    Observed = element(2,lists:nth(1,payload_to_list([],Payload))),
                    Expected = element(2,lists:max([{Y,X} || {Y,X} <- Data, Y=<Timestamp])),
                    Expected =:= Observed;
                _ ->
                    false
            end;
        _ ->
            true
    end;
postcondition(_S, _V, _Result) ->
    true.

%%-----------------------------------------------------------------------------
%% properties
%%-----------------------------------------------------------------------------

prop_cake_protocol_works() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            application:start(cake),
            {History,State,Result} = run_commands(?MODULE, Cmds),
            application:stop(cake),
            [proper_utils:cleanup(binary_to_list(X)) || {_,X,_} <- State#state.streams],
            ?WHENFAIL(
                io:format("\n\nHistory: ~w\n\nState: ~w\n\nResult: ~w\n\n",
                [History,State,Result]),
                aggregate(command_names(Cmds), Result =:= ok))
        end).

%%-----------------------------------------------------------------------------
%% query operations
%%-----------------------------------------------------------------------------

request_stream_with_size(Size,StreamName) ->
    Message = list_to_binary([ <<Size:16>>,StreamName]),
    tcp_query(?HOST,?PORT,packet(?REQUEST_STREAM_WITH_SIZE,Message)).

append(StreamID,String) ->
    Message = list_to_binary([ <<StreamID:16>>,String]),
    tcp_query(?HOST,?PORT,packet(?APPEND,Message)).

simple_query(StreamID,Start,End) ->
    timer:sleep(?TIMEOUT), % Give CakeDB time to flush
    Message = list_to_binary([ <<StreamID:16>>, Start, End]),
    tcp_query(?HOST,?PORT,packet(?QUERY,Message)).

all_since(StreamID,Time) ->
    timer:sleep(?TIMEOUT), % Give CakeDB time to flush
    Message = list_to_binary([ <<StreamID:16>>, Time]),
    tcp_query(?HOST,?PORT,packet(?ALL_SINCE,Message)).

request_stream(StreamName) ->
    tcp_query(?HOST,?PORT,packet(?REQUEST_STREAM,StreamName)).
 
last_entry_at(StreamID,Time) ->
    timer:sleep(?TIMEOUT), % Give CakeDB time to flush
    Message = list_to_binary([ <<StreamID:16>>, Time]),
    tcp_query(?HOST,?PORT,packet(?LAST_ENTRY_AT,Message)).

%%-----------------------------------------------------------------------------
%% utils
%%-----------------------------------------------------------------------------

% Send a binary query to CakeDB and listen for result
tcp_query(Host,Port,Packet) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active,false}]),
    gen_tcp:send(Socket, Packet),
    {Flag,Result} = gen_tcp:recv(Socket,0,500), 
    case Flag of
        ok ->
            case byte_size(Result) of
                2 ->
                    <<Output:16/integer>> = Result;
                _ ->
                    <<Length:32/integer, Payload/binary>> = Result,
                    case byte_size(Payload) < Length of
                        true ->
                            Output = gen_tcp:recv(Socket,Length,500);
                        false ->
                            Output = {ok,Payload}
                    end
            end;
        error ->
            case Result of
                closed ->
                    Output = unregistered_stream;
                timeout ->
                    Output = casted
            end
    end,
    gen_tcp:close(Socket),
    Output.

% Returns a binary packet of type Option
% to be sent to CakeDB
packet(Option,Message) ->
    Length = int_to_binary(size(Message),32),
    list_to_binary([Length, <<Option:16>>,Message]).

% Returns the current timestamp, in microseconds
% since epoch, in a 8 bytes binary
timestamp() ->
    {Mega, Sec, Micro} = now(),
    TS = Mega * 1000000 * 1000000 + Sec * 1000000 + Micro,
    <<TS:64/big-integer>>.

% Returns symbolic call to timestamp/0 function
timestamp_wrapper() ->
    {call,?MODULE,timestamp,[]}.

% Take a binary CakeDB payload and returns a
% list of datapoints of the form
% [{TS_1,DATA_1}, ... , {TS_N,DATA_N}]
payload_to_list(List, Payload) ->
    case Payload of
        <<>> ->
            List;
        <<TS:64,Length:32,Rest/binary>> ->
            Bits = 8*Length,
            <<Data:Bits, Other/binary>> = Rest,
            payload_to_list(List++[{TS,binary_to_list(<<Data:Bits>>)}], Other)
    end.

% Takes an integer and returns its <<Bits>>
% bits binary representation
int_to_binary(Int, Bits) ->
    <<Int:Bits>>.

