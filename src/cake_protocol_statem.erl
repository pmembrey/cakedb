-module(cake_protocol_statem).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-compile(export_all).

-export([initial_state/0, command/1, precondition/2, postcondition/3,
        next_state/3]).

% E.g. of model state structure:
% {
%   starttime = 1355417136196,
%   streams = [
%     {StreamID_1, StreamName_1, [{TS_1,<<"DATA_1">>}, ..., {TS_M,<<"DATA_M">>}]},
%     ...,
%     {StreamID_N, StreamName_N, [{TS_1,<<"DATA_1">>}, ..., {TS_L,<<"DATA_L">>}]}
%   ]
% }
-record(state,{starttime,streams}).

% Options
-define(NOOP, 0).
-define(REQUEST_STREAM_WITH_SIZE, 1).
-define(APPEND, 2).
-define(QUERY, 3).
-define(ALL_SINCE, 4).
-define(REQUEST_STREAM, 5).
-define(LAST_ENTRY_AT, 6).

% Buffer zone of difference in ms between a
% timestamp in the model state and a timestamp
% in the database
-define(ERROR_MARGIN, 100).

% socket
-define(HOST,"localhost").
-define(PORT,8888).

-define(STREAMNAMES, ["tempfile", "file001", "anotherfile",
        "somefile", "binfile", "cakestream"]).

%%-----------------------------------------------------------------------------
%% statem callbacks
%%-----------------------------------------------------------------------------

% initialize the state machine
initial_state() ->
    #state{
            starttime = timestamp(),
            streams = []
          }.

% define the commands to test
command(S) ->
    T0 = S#state.starttime,
    T1 = timestamp(),
    oneof([
        {call,?MODULE,request_stream_with_size,[pos_integer(),streamname()]},
        {call,?MODULE,append,[streamid(S),list(integer(32,255))]},
        {call,?MODULE,simple_query,[streamid(S),uniform(T0,T1),uniform(T0,T1)]},
        {call,?MODULE,all_since,[streamid(S),uniform(T0,T1)]},
        {call,?MODULE,request_stream,[streamname()]},
        {call,?MODULE,last_entry_at,[streamid(S),uniform(T0,T1)]}
    ]).

% define when a command is valid
precondition(_S, _Command) ->
    true. % all preconditions are valid

%% define the state transitions triggered
%% by each command
%next_state(S,{ok,<<_,ID>>},{call,?MODULE,request_stream_with_size,[_Size,StreamName]}) ->
%    S#state{
%        streams = [{ID, StreamName, []}|S#state.streams]
%    };
%next_state(S,{ok,<<_,ID>>},{call,?MODULE,request_stream,[StreamName]}) ->
%    S#state{
%        streams = [{ID, StreamName, []}|S#state.streams]
%    };
%% all the other commands do not change the abstract state
next_state(S, _V, _Command) ->
    S.

% define the conditions needed to be
% met in order for a test to pass
postcondition(_S, _Command, _Result) ->
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
            [cake_streams_statem:cleanup(X) || {_,X,_} <- element(3,State)],
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
streamid(#state{streams = []}) ->
    1;
streamid(#state{streams = Streams}) ->
    IDs = [X || {X,_,_} <- Streams],
    elements([lists:max(IDs)+1|IDs]).

uniform(A,B) ->
    % Returns random integer in range [A,B]
    case A=:=B of
        true ->
            A;
        false ->
            A + random:uniform(B-A)
    end.

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
    Message = list_to_binary([ <<StreamID:16>>, <<Start:64>>, <<End:64>>]),
    tcp_query(?HOST,?PORT,packet(?QUERY,Message)).

all_since(StreamID,Time) ->
    Message = list_to_binary([ <<StreamID:16>>, <<Time:64>>]),
    tcp_query(?HOST,?PORT,packet(?ALL_SINCE,Message)).

request_stream(StreamName) ->
    Message = list_to_binary(StreamName),
    tcp_query(?HOST,?PORT,packet(?REQUEST_STREAM,Message)).
 
last_entry_at(StreamID,Time) ->
    Message = list_to_binary([ <<StreamID:16>>, <<Time:64>>]),
    tcp_query(?HOST,?PORT,packet(?LAST_ENTRY_AT,Message)).

%%-----------------------------------------------------------------------------
%% utils
%%-----------------------------------------------------------------------------

tcp_query(Host,Port,Packet) ->
    % Send a binary query to CakeDB and listen for result
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active,false}]),
    gen_tcp:send(Socket, Packet),
    Output = gen_tcp:recv(Socket,0,500),
    gen_tcp:close(Socket),
    io:format("~nOutput: ~p~n~n",[Output]),
    Output.

packet(Option,Message) ->
    % Returns a binary packet of type Option
    % to be sent to CakeDB
    Length = int_to_binary(size(Message),32),
    list_to_binary([Length, <<Option:16>>,Message]).

timestamp() ->
    % Returns time in milliseconds since epoch
    {MegaSecs,Secs,MicroSecs} = now(),
    1000000000*MegaSecs + 1000*Secs + MicroSecs div 1000.

int_to_binary(Int, Bits) ->
    <<Int:Bits>>.

