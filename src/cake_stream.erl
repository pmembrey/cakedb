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




-export([init/3,writer_init/3,timestamp_as_native_binary/0,verify_file/1]).

init(Stream,StreamID,SliceName) ->

	register(list_to_atom(binary_to_list(Stream) ++ "_master"),self()),
	lager:debug("Started with destination pid '~p'",[self()]),


	lager:info("Creating stream ~p registered as ~p",[Stream,StreamID]),



	Writer = spawn_link(?MODULE,writer_init,[self(),Stream,SliceName]),

	register(list_to_atom(binary_to_list(Stream) ++ "_writer"),Writer),


	gproc:add_local_name({stream,StreamID}),
	loop(Writer,[],true,0,0,Stream).

loop(Writer,DataList,ClearToSend,LastTS,Count,StreamName) ->
        case length(DataList) > 0 of
		true ->
                        case ClearToSend of
				true ->
					lager:debug("Now clear to send - sending what we have..."),
					Writer ! {Count,LastTS,lists:reverse(DataList)},
					loop(Writer,[],false,LastTS,0,StreamName);
				false ->
					lager:debug("Have data, waiting for clear to send")
			end;
		false ->
                        lager:debug("Have no data, listening")
	end,	
	receive
		Message ->
			case Message of 
				clear ->
					lager:debug("Got 'clear' message on ~p",[self()]),
					loop(Writer,DataList,true,LastTS,Count,StreamName);

				Data ->

					TS = timestamp_as_native_binary(),
					
					PayloadLength = erlang:byte_size(Data),
					Store   = <<1,1,1,TS/binary,PayloadLength:32/native-integer,(erlang:crc32(Data)):32/native-integer,2,2,2,Data/binary,3,3,3>>,

					NewDataList = [Store|DataList],

					case ClearToSend of
						true ->
							lager:debug("Got data and we're clear to send!"),
							Writer ! {Count,TS,lists:reverse(NewDataList)},
							loop(Writer,[],false,TS,0,StreamName);
						false ->
						    case Count > 50000 of
                            	true ->
                                	lager:warning("~p(~p) has more than 50,000 messages pending - sending message by force!",[StreamName,self()]),
                                    Writer ! {Count,TS,lists:reverse(NewDataList)},
                                    loop(Writer,[],false,TS,0,StreamName);
                                false -> ok
                            end,
							lager:debug("Got data but not clear to send!"),
							loop(Writer,NewDataList,false,TS,Count+1,StreamName)
					end
			end
	after 5000 ->
		% Routine maintenance 
		erlang:garbage_collect(),
                loop(Writer,DataList,ClearToSend,LastTS,Count,StreamName)
	end.





writer_init(From,Stream,SliceName) ->
	% First ensure stream directory exists...
	{ok,DataDir} = application:get_env(cake,data_dir),
	{ok,WriteDelay} = application:get_env(cake,write_delay),
	Path = DataDir ++ binary_to_list(Stream) ++ "/",
	filelib:ensure_dir(Path),
	DataFile  = Path ++ SliceName ++ ".data",
	IndexFile = Path ++ SliceName ++ ".index",
	lager:debug("Data File:   ~p",[DataFile]),
	lager:debug("Index File:  ~p",[IndexFile]),
        % Wait for 50MB or WriteDelay milliseconds before flushing
	{ok,File}   = file:open(DataFile,[append,raw,{delayed_write, 50*1024*1024, WriteDelay}]),
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



%%%
%%% Experimental start to file recovery...
%%%

verify_file(FileName) ->
    {ok,DataFile} = file:open(FileName,[write,read,binary,raw]),
    {ok,Offset} = file:position(DataFile,{eof,-3}),

    case file:read(DataFile,3) of
    	{ok,<<3,3,3>>} -> lager:info("~p is okay",[FileName]);
    	Data           -> lager:warning("~p is corrupt: ~p",[FileName,Data]),
    					  fix_file(DataFile,Offset,FileName)

    end,
    file:close(DataFile).


fix_file(DataFile,Offset,FileName) ->
	{ok,NewOffset} = file:position(DataFile,{cur,-4}),
	 case file:read(DataFile,3) of
    	{ok,<<3,3,3>>} -> lager:info("Last message found at ~p in ~p, truncating file...",[Offset,FileName]),
    					  file:truncate(DataFile);
    	_Data          -> fix_file(DataFile,NewOffset,FileName)
    end.



