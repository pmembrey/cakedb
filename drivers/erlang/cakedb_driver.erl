-module(cakedb_driver).
-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

-export([start_link/0,stop/0,append/2,range_query/3,all_since/2,last_entry_at/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% Options
-define(REQUEST_STREAM_WITH_SIZE, 1).
-define(APPEND, 2).
-define(QUERY, 3).
-define(ALL_SINCE, 4).
-define(REQUEST_STREAM, 5).
-define(LAST_ENTRY_AT, 6).

% socket
-define(HOST,"localhost").
-define(PORT,8888).
-define(TIMEOUT,1000).

% E.g. of state structure:
% {
%   socket = #Port<0.556>,
%   streams = [
%     {StreamID_1, StreamName_1},
%     ...,
%     {StreamID_N, StreamName_N}
%   ]
% }
-record(state,{socket,streams}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% CLIENT API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Synchronous calls
range_query(StreamName,Start,End) ->
    gen_server:call(?MODULE, {range_query,StreamName,Start,End}).

all_since(StreamName,Time) ->
    gen_server:call(?MODULE, {all_since,StreamName,Time}).

last_entry_at(StreamName,Time) ->
    gen_server:call(?MODULE, {last_entry_at,StreamName,Time}).

stop() -> 
    gen_server:call(?MODULE, stop).

% Asynchronous casts
append(StreamName,Data) ->
    gen_server:cast(?MODULE, {append,StreamName,Data}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% DRIVER FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    lager:info("Started basic Cake driver"),
    {ok,Socket} = gen_tcp:connect(?HOST,?PORT,[binary,{packet, raw},{active,false}]),
    {ok, #state{socket=Socket, streams=[]}}.

handle_call({range_query,StreamName,Start,End}, _From, State) ->
%    io:format("~nIn range_query handle_call, StreamName: ~p, State: ~p~n~n",[StreamName,State]),
    case lists:keysearch(StreamName,2,State#state.streams) of
        {value, {StreamID,StreamName}} ->
            Message = list_to_binary([<<StreamID:16/big-integer>>,
                    <<Start:64/big-integer>>, <<End:64/big-integer>>]),
            Payload = cake_query(State#state.socket, ?QUERY, Message),
            {reply, Payload, State};
        false ->
            {reply, [], State}
    end;

handle_call({all_since, StreamName, Time}, _From, State) ->
    case lists:keysearch(StreamName,2,State#state.streams) of
        {value, {StreamID,StreamName}} ->
            Message = list_to_binary([<<StreamID:16>>, <<Time:64/big-integer>>]),
            Payload = cake_query(State#state.socket, ?ALL_SINCE, Message),
            {reply, Payload, State};
        false ->
            {reply, [], State}
    end;

handle_call({last_entry_at, StreamName, Time}, _From, State) ->
    case lists:keysearch(StreamName,2,State#state.streams) of
        {value, {StreamID,StreamName}} ->
            Message = list_to_binary([<<StreamID:16>>, <<Time:64/big-integer>>]),
            Payload = cake_query(State#state.socket, ?LAST_ENTRY_AT, Message),
            {reply, Payload, State};
        false ->
            {reply, [], State}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State}.

handle_cast({append, StreamName, Data}, State) ->
    case lists:keysearch(StreamName,2,State#state.streams) of
        {value, {StreamID,StreamName}} ->
            ok;
        false ->
            StreamID = request_stream(State#state.socket,StreamName)
    end,
    Message = list_to_binary([<<StreamID:16>>,Data]),
    Packet = packet(?APPEND,Message),
    gen_tcp:send(State#state.socket, Packet),
    {noreply, State#state{streams = [{StreamID, StreamName}|State#state.streams]}}.

handle_info(Msg, State) ->
    lager:info("Unexpected message: ~p~n",[Msg]),
    {noreply, State}.

terminate(normal, State) ->
    gen_tcp:close(State#state.socket),
    lager:info("Stopped basic Cake driver"),
    ok.

code_change(_OldVsn, State, _Extra) ->
    %% No change planned. The function is there for the behaviour,
    %% but will not be used. Only a version on the next
    {ok, State}. 

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% UTILS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

request_stream(Socket,StreamName) ->
    Message = list_to_binary([StreamName]),
    Packet = packet(?REQUEST_STREAM,Message),
    gen_tcp:send(Socket, Packet),
    {ok,<<StreamID:16/integer>>} = gen_tcp:recv(Socket,0,?TIMEOUT), 
    StreamID.

request_stream_with_size(Socket,Size,StreamName) ->
    Message = list_to_binary([ <<Size:16>>,StreamName]),
    Packet = packet(?REQUEST_STREAM_WITH_SIZE,Message),
    gen_tcp:send(Socket, Packet),
    {ok,<<StreamID:16/integer>>} = gen_tcp:recv(Socket,0,?TIMEOUT), 
    StreamID.

% Send a query to CakeDB and listen for result
cake_query(Socket, QueryID, Message) ->
    Packet = packet(QueryID,Message),
    gen_tcp:send(Socket, Packet),
    {ok,<<Length:32/integer>>} = gen_tcp:recv(Socket,4,?TIMEOUT), 
    case Length of
        0 ->
            [];
        _ ->
            {ok,Payload} = gen_tcp:recv(Socket,Length,?TIMEOUT),
            payload_to_list(Payload)
    end.

% Returns a binary packet of type Option
% to be sent to CakeDB
packet(Option,Message) ->
    Length = int_to_binary(size(Message),32),
    list_to_binary([Length, <<Option:16>>,Message]).

% Takes an integer and returns its <<Bits>>
% bits binary representation
int_to_binary(Int, Bits) ->
    <<Int:Bits>>.

% Take a binary CakeDB payload and returns a
% list of datapoints of the form
% [{TS_1,DATA_1}, ... , {TS_N,DATA_N}]
payload_to_list(Payload) ->
    payload_to_list([],Payload).

payload_to_list(List, Payload) ->
    case Payload of
        <<>> ->
            List;
        <<TS:64,Length:32,Rest/binary>> ->
            Bits = 8*Length,
            <<Data:Bits, Other/binary>> = Rest,
            payload_to_list(List++[{TS,binary_to_list(<<Data:Bits>>)}], Other)
    end.

