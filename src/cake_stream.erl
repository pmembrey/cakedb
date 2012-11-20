-module(cake_stream).
-compile([{parse_transform, lager_transform}]).

-include_lib("kernel/include/file.hrl").


-define(SIZE_INDEX,1).
-define(COUNT_INDEX,2).




-record(stream_state,{	from,
						data_file,
						index_file,
						stream_name,
						total_bytes,
						byte_counter,
						last_ts,
						message_count
						}).




-export([init/3,writer_init/3,timestamp_as_native_binary/0]).

init(Stream,StreamID,SliceName) ->

	register(list_to_atom(binary_to_list(Stream) ++ "_master"),self()),
	lager:debug("Started with destination pid '~p'",[self()]),


	lager:info("Creating stream ~p registered as ~p",[Stream,StreamID]),



	Writer = spawn_link(?MODULE,writer_init,[self(),Stream,SliceName]),

	register(list_to_atom(binary_to_list(Stream) ++ "_writer"),Writer),


	gproc:add_local_name({stream,StreamID}),
	loop(Writer,[],true,0,0).

loop(Writer,DataList,ClearToSend,LastTS,Count) ->
	receive
		Message ->
			case Message of 
				clear ->
					lager:debug("Got 'clear' message on ~p",[self()]),
					loop(Writer,DataList,true,LastTS,Count);

				Data ->

					{ok,CompressedData} = snappy:compress(Data),
					TS = timestamp_as_native_binary(),
					
					PayloadLength = erlang:byte_size(CompressedData),
					Store = <<TS/binary,PayloadLength:32/native-integer,CompressedData/binary>>,
					NewDataList = [Store|DataList],

					case ClearToSend of
						true ->
							lager:debug("Got data and we're clear to send!"),
							Writer ! {Count,TS,lists:reverse(NewDataList)},
							loop(Writer,[],false,TS,0);
						false ->
							lager:debug("Got data but not clear to send!"),
							loop(Writer,NewDataList,false,TS,Count+1)
					end
			end
	after 5000 ->
		% Routine maintenance 
		erlang:garbage_collect(),
		case ClearToSend of
			true ->
				case length(DataList) > 0 of
					true ->
						lager:debug("Clear to send but no new data in the last 5 seconds - sending what we have..."),
						Writer ! {Count,LastTS,lists:reverse(DataList)},
						loop(Writer,[],false,LastTS,0);
					false ->
						lager:debug("Clear to send but no data..."),
						loop(Writer,[],true,LastTS,Count)
				end;
			false ->
				loop(Writer,DataList,false,LastTS,Count)
		end	
	end.





writer_init(From,Stream,SliceName) ->
	% First ensure stream directory exists...
	Path = "data/" ++ binary_to_list(Stream) ++ "/",
	filelib:ensure_dir(Path),
	DataFile  = Path ++ SliceName ++ ".data",
	IndexFile = Path ++ SliceName ++ ".index",
	lager:debug("Data File:   ~p",[DataFile]),
	lager:debug("Index File:  ~p",[IndexFile]),
	{ok,File}   = file:open(DataFile,[append,raw,{delayed_write, 50*1024*1024, 10000}]),  % Wait for 50MB of 10 seconds before flushing
	{ok,FileInfo} = file:read_file_info(DataFile),
	{ok,Index}  = file:open(IndexFile,[append,raw]),

	Details = #stream_state{
					from            = From,
					data_file       = File,
					index_file      = Index,
					stream_name     = Stream,
					total_bytes     = (FileInfo#file_info.size),
					byte_counter    = 0,
					last_ts         = 0,
					message_count   = 0
					},


	writer(Details).

writer(Details) ->
	% 50MB (52428800 bytes) 
	case (Details#stream_state.byte_counter) > 52428800 of
		true ->
			lager:debug("Adding index for '~p' at '~p' after 50MB written",[Details#stream_state.stream_name,Details#stream_state.last_ts]),
			ok = file:write((Details#stream_state.index_file),<<(Details#stream_state.last_ts)/binary,?SIZE_INDEX:8/native-integer,(Details#stream_state.total_bytes):64/native-integer>>),
			writer(Details#stream_state{byte_counter=0});
		false -> 
			case (Details#stream_state.message_count) > 1000 of
				true ->
					lager:debug("Adding index for '~p' at '~p' after 1,000 inserts",[Details#stream_state.stream_name,Details#stream_state.last_ts]),
					ok = file:write((Details#stream_state.index_file),<<(Details#stream_state.last_ts)/binary,?COUNT_INDEX:8/native-integer,(Details#stream_state.total_bytes):64/native-integer>>),
					writer(Details#stream_state{message_count=0});
				false -> ok
			end
	end,
	receive
		{Count,TS,Data} ->
			lager:debug("Got data to write!"),
			DataToWrite = list_to_binary(lists:flatten(Data)),
			ok = file:write(Details#stream_state.data_file,DataToWrite),
			lager:debug("Wrote data - now sending clear..."),
			(Details#stream_state.from) ! clear,
			lager:debug("Clear sent!"),
			lager:debug("Bytes: ~p",[Details#stream_state.byte_counter]),
			writer(Details#stream_state{total_bytes = (Details#stream_state.total_bytes) + byte_size(DataToWrite),last_ts = TS, byte_counter = (Details#stream_state.byte_counter) + byte_size(DataToWrite),message_count = (Details#stream_state.message_count) + Count})

	after 5000 ->
		% Routine maintenance 
		erlang:garbage_collect(),
		writer(Details)
	end.



timestamp_as_native_binary() ->
        {Mega, Sec, Micro} = now(),
        TS = Mega * 1000000 * 1000000 + Sec * 1000000 + Micro,
        <<TS:64/native-integer>>.



