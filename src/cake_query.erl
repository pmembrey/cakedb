-module(cake_query).
-compile([{parse_transform, lager_transform}]).

-export([all_since_query/2,
         dump_index/1,
         retrieve_last_entry_at/2,
         simple_query/3]).


-define(MESSAGE_HEADER,<<1,1,1,TS:64/native-integer,Size:32/native-integer,CRC32:32/native-integer,2,2,2>>).
-define(MESSAGE,<<Data:Size/binary,3,3,3>>).

%% Thoughs on defining MESSAGE_PACKAGE as [?MESSAGE_HEADER, ?MESSAGE] or <<?MESSAGE_HEADER, ?MESSAGE>> for evolution.
%% At this time, only TS, Size and Data are necessary for operation functions.
%% CRC32 is processed at an upper level.
-define(MESSAGE_PACKAGE, [TS, Size, Data]).

%% Quick work around for handling decompressed data
-define(MESSAGE_PACKAGE_DECOMPRESSED, [TS, DecompressedSize, Decompressed]).

retrieve_last_entry_at(StreamID,At) ->
    retrieve_data(retrieve_last_entry_at, StreamID, [At])
    .

simple_query(StreamID,From,To) ->
    retrieve_data(simple_query, StreamID, [From, To])
    .

all_since_query(StreamID,From) ->
    retrieve_data(all_since_query, StreamID, [From])
    .



open_data_file(FileName) ->
    {ok,DataDir} = application:get_env(cake,data_dir),
    {ok,DataFile} = file:open(DataDir ++ FileName ++ "/" ++ FileName ++ ".data",[binary,raw,read,read_ahead]),
    DataFile.

retrieve_data(Operation, StreamID, ConditionList) ->
    [At | _] = ConditionList,
    DataFile = retrieve_data_init(StreamID, At),
    FoundData = retrieve_data(Operation, DataFile, [], ConditionList),
    ok = file:close(DataFile),
    list_to_binary(lists:flatten(lists:reverse(FoundData)))
    .

retrieve_data_init(StreamID, At) ->
    {ok, File} = cake_stream_manager:stream_filename(StreamID),
    Offset = get_indexed_offset(StreamID, At),
    lager:debug("Offset from get_indexed_offset: ~p", [Offset]),
    DataFile = open_data_file(File),
    {ok, Offset} = file:position(DataFile, Offset),
    DataFile
    .

%% Common recursive part for each query
retrieve_data(Operation, DataFile, FoundData, ConditionList) ->
    case file:read(DataFile, 22) of
        {ok, ?MESSAGE_HEADER} ->
            case file:read(DataFile, Size+3) of
                {ok,?MESSAGE} -> case CRC32 == erlang:crc32(Data) of
                                                true -> %case snappy:decompress(Data) of
                                                        %    {ok,Decompressed} -> DecompressedSize = byte_size(Decompressed),
                                                        %                 retrieve_data(Operation, DataFile, FoundData, ConditionList, ?MESSAGE_PACKAGE_DECOMPRESSED);
                                                        %    SomethingElse -> lager:warning("Decompression Failed on ~p : '~p' -- ~p",[TS,SomethingElse,Data]),
                                                        %    retrieve_data(Operation, DataFile, FoundData, ConditionList)
                                                        %end;
                                                        retrieve_data(Operation,DataFile,FoundData,ConditionList,?MESSAGE_PACKAGE);
                                                false -> lager:warning("Message: CRC32 Checksum failed on ~p", [TS]),
                                                         retrieve_data(Operation, DataFile, FoundData, ConditionList)
                                end;
                {ok,_BadData}  -> lager:warning("Corrupt message data detected! Skipping to next valid header!"),
                                 NewDataFile = find_next_header(DataFile),
                                 retrieve_data(Operation,NewDataFile,FoundData,ConditionList);
                eof -> lager:warning("EOF: Not enough data - attempted to read ~p bytes", [Size]),
                       FoundData
            end;
        {ok,BadData}  -> lager:warning("Corrupt message header detected! Skipping to next valid header! ~p",[BadData]),
                          NewDataFile = find_next_header(DataFile),
                          retrieve_data(Operation,NewDataFile,FoundData,ConditionList);
        _ -> FoundData
    end
    .

retrieve_data(retrieve_last_entry_at, DataFile, FoundData, [At], ?MESSAGE_PACKAGE) ->
    case TS < At of
	true -> retrieve_data(retrieve_last_entry_at, DataFile, [<<TS:64/big-integer, Size:32/big-integer, Data/binary>>], [At]);
        false -> FoundData
    end;

retrieve_data(simple_query, DataFile, FoundData, [From, To], ?MESSAGE_PACKAGE) ->
    case (TS >= From) and (TS =< To) of
	true -> retrieve_data(simple_query, DataFile, [<<TS:64/big-integer, Size:32/big-integer, Data/binary>> | FoundData], [From, To]);
	false -> case TS < To of 
		     true -> retrieve_data(simple_query, DataFile, FoundData, [From, To]);
                     false -> FoundData
		 end
    end;

retrieve_data(all_since_query, DataFile, FoundData, [From], ?MESSAGE_PACKAGE) ->
    case TS > From of
	true -> retrieve_data(all_since_query, DataFile, [<<TS:64/big-integer, Size:32/big-integer, Data/binary>> | FoundData], [From]);
        false -> retrieve_data(all_since_query, DataFile, FoundData, [From])
    end
    .















dump_index(FileName) ->
    {ok,Data} = file:read_file(FileName),
    dump_index(Data,0).


dump_index(<<>>,Count) ->
    io:format("Total records: ~p~n",[Count]),
    ok;
dump_index(Data,Count) ->
    <<TS:64/native-integer,Type:8/native-integer,ByteLocation:64/native-integer,TheRest/binary>> = Data,
    
    io:format("~p|~p|~p~n",[TS,Type,ByteLocation]),
    dump_index(TheRest,Count + 1).



get_indexed_offset(StreamID,From) ->
    {ok,File} = cake_stream_manager:stream_filename(StreamID),
    {ok,DataDir} = application:get_env(cake,data_dir),
    {ok,Data} = file:read_file(DataDir ++ File ++ "/" ++ File ++ ".index"),
    get_indexed_offset(Data,From,0).

get_indexed_offset(<<>>,_From,Offset) ->
    Offset;
get_indexed_offset(Data,From,Offset) ->
    <<TS:64/native-integer,_Type:8/native-integer,ByteLocation:64/native-integer,TheRest/binary>> = Data,
    case From > TS of
        true -> get_indexed_offset(TheRest,From,ByteLocation);
        false -> Offset
    end.


find_next_header(DataFile) ->
   case file:read(DataFile,3) of
        {ok,<<1,1,1>>}   -> {ok,_Offset} = file:position(DataFile,{cur,-3}),
                            DataFile;
        {ok,_Data}        -> {ok,_Offset} = file:position(DataFile,{cur,-2}),
                            find_next_header(DataFile);
        eof              -> DataFile
    end.





