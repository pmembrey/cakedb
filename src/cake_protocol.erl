-module(cake_protocol).
-export([start_link/4, init/4]).
-export([timestamp/0]).
-compile([{parse_transform, lager_transform}]).


% Options flags for reference
-define(NOOP,0).
-define(REQUEST_STREAM_WITH_SIZE,1).
-define(APPEND,2).
-define(QUERY,3).
-define(ALL_SINCE,4).
-define(REQUEST_STREAM,5).
-define(LAST_ENTRY_AT,6).


start_link(ListenerPid, Socket, Transport, Opts) ->
	Pid = spawn_link(?MODULE, init, [ListenerPid, Socket, Transport, Opts]),
	{ok, Pid}.

init(ListenerPid, Socket, Transport, _Opts = []) ->
	ok = ranch:accept_ack(ListenerPid),
	loop(Socket, Transport).

loop(Socket, Transport) ->
	% Wait for the 6 byte header
	case Transport:recv(Socket, 6,infinity) of
		{ok, <<Length:32/big-integer,Type:16/big-integer>>} ->
			lager:debug("Length: ~p~nType: ~p~n",[Length,Type]),
			{ok,<<Message:Length/binary>>} = Transport:recv(Socket,Length,infinity),

			folsom_metrics:notify({<<"msg_per_sec">>, 1}),

			%statsderl:increment("total_inbound_messages",1, 0.005),


			case Type of
                ?NOOP ->
                    lager:debug("NOOP"),
					loop(Socket,Transport);


				?REQUEST_STREAM_WITH_SIZE ->
					lager:debug("Client wants a stream!"),
					<<_StreamLength:16/big-integer,StreamName/binary>> = Message,
					lager:debug("StreamName: ~p~n",[StreamName]),
					StreamID = cake_stream_manager:register_stream(StreamName),
					Transport:send(Socket,<<StreamID:16/big-integer>>),
					loop(Socket,Transport);


				?APPEND ->
					lager:debug("Append request!"),
					<<StreamID:16/big-integer,Payload/binary>> = Message,
					case gproc:lookup_local_name({stream,StreamID}) of
						undefined ->
							lager:warning("Message for non-existent stream ~p.",[StreamID]),
							ok = Transport:close(Socket);
						_ ->
							gproc:send({n,l,{stream,StreamID}},Payload),
							loop(Socket,Transport)
					end;

				?QUERY ->
					lager:info("Query request!"),
					<<StreamID:16/big-integer,From:64/big-integer,To:64/big-integer>> = Message,
					lager:info("StreamID: ~p~nFrom: ~p~nTo:   ~p~n",[StreamID,From,To]),
					Data = cake_query:simple_query(StreamID,From,To),
					DataLength = byte_size(Data),
					Transport:send(Socket,<<DataLength:32/big-integer>>),
					Transport:send(Socket,Data),
					loop(Socket,Transport);


				?ALL_SINCE ->
					lager:debug("All Since Query request!"),
					<<StreamID:16/big-integer,From:64/big-integer>> = Message,
					lager:debug("StreamID: ~p~nFrom: ~p~n",[StreamID,From]),
					Data = cake_query:all_since_query(StreamID,From),
					DataLength = byte_size(Data),
					Transport:send(Socket,<<DataLength:32/big-integer>>),
					Transport:send(Socket,Data),
					loop(Socket,Transport);


				?REQUEST_STREAM ->
					lager:debug("Client wants a stream!"),
					<<StreamName/binary>> = Message,
					lager:debug("StreamName: ~p~n",[StreamName]),
					StreamID = cake_stream_manager:register_stream(StreamName),
					Transport:send(Socket,<<StreamID:16/big-integer>>),
					loop(Socket,Transport);

				?LAST_ENTRY_AT ->
					lager:debug("Last Entry request!"),
					<<StreamID:16/big-integer,At:64/big-integer>> = Message,
					lager:debug("StreamID: ~p~nFrom: ~p~n",[StreamID,At]),
					Data = cake_query:retrieve_last_entry_at(StreamID,At),
					DataLength = byte_size(Data),
					Transport:send(Socket,<<DataLength:32/big-integer>>),
					Transport:send(Socket,Data),
					loop(Socket,Transport);


				_ ->
					lager:warning("Received invalid message type - disconnecting client."),
					ok = Transport:close(Socket)
			end;


		Failed ->
					lager:info("Socket closed: ~p",[Failed]),
					ok = Transport:close(Socket)

	end.



timestamp() ->
        {Mega, Sec, Micro} = now(),
        Mega * 1000000 * 1000000 + Sec * 1000000 + Micro.
