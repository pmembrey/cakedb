-module(cakedb_driver).
-compile([{parse_transform, lager_transform}]).


-export([loop/2,init/0]).

init() ->
        lager:info("Started basic Cake driver"),

        {ok, Socket} = gen_tcp:connect("localhost", 8888,
                                 [binary, {packet, raw},{active,false}]),
        loop(Socket,[]).


loop(Socket,Streams) ->
        receive
                {Stream,Message} ->

                case proplists:is_defined(Stream,Streams) of
                        true ->
                                StreamID = proplists:get_value(Stream,Streams),
                                NewStreams = Streams;
                        false ->
                                StreamNameLength = byte_size(Stream),
                                RequestType = 1, % Stream register request
                                StreamLength = StreamNameLength + 2, % Binary data plus Stream Name Length
                                RequestPayload = <<StreamLength:32/big-integer,RequestType:16/big-integer,StreamNameLength:16/big-integer,Stream/binary>>,
                                ok = gen_tcp:send(Socket,RequestPayload),
                                {ok,Response} = gen_tcp:recv(Socket,2,infinity),
                                <<StreamID:16/big-integer>> = Response,
                                NewStreams = [{Stream,StreamID} | Streams]
                end,

                Length = byte_size(Message) +2, % Binary data plus 16 bit stream ID
                Type =2, % Append 
                Payload = <<Length:32/big-integer,Type:16/big-integer,StreamID:16/big-integer,Message/binary>>,
                ok = gen_tcp:send(Socket,Payload),
                loop(Socket,NewStreams)
        end.