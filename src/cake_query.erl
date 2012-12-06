-module(cake_query).
-compile([{parse_transform, lager_transform}]).

-export([return_all/1,
         all_since_query/2,
         dump_index/1,
         retrieve_last_entry_at/2,
         simple_query/3]).


return_all(FileName) ->
	{ok,Data} = file:read_file(FileName),
	print_all(Data,0).


print_all(<<>>,Count) ->
	io:format("Total records: ~p~n",[Count]),
	ok;
print_all(Data,Count) ->
	<<TS:64/native-integer,Size:32/native-integer,Compressed:Size/binary,TheRest/binary>> = Data,
	{ok,Decompressed} = snappy:decompress(Compressed),
	io:format("~p|~p|~p~n",[TS,Size,Decompressed]),
	print_all(TheRest,Count + 1).



open_data_file(FileName) ->
    {ok,DataDir} = application:get_env(cakedb,data_dir),
    {ok,DataFile} = file:open(DataDir ++ FileName ++ "/" ++ FileName ++ ".data",[binary,raw,read,read_ahead]),
    DataFile.





retrieve_last_entry_at(StreamID,At) ->
    File = cake_stream_manager:stream_filename(StreamID),
    Offset = get_indexed_offset(StreamID,At),
    lager:debug("Offset from get_indexed_offset: ~p", [Offset]),
    DataFile = open_data_file(File),
    {ok,Offset} = file:position(DataFile,Offset),
    retrieve_last_entry_at(DataFile,[],At).


retrieve_last_entry_at(DataFile,FoundData,At) ->
    case file:read(DataFile,12) of
        {ok,<<TS:64/native-integer,Size:32/native-integer>>} ->
            case file:read(DataFile,Size) of
                {ok,Data} -> case Size == byte_size(Data) of
                                                true -> case TS > At of
                                                            true -> retrieve_last_entry_at(DataFile,<<TS:64/big-integer,Size:32/big-integer,Data/binary>>,At);
                                                            false -> retrieve_last_entry_at(DataFile,FoundData,At)
                                                        end;
                                                false -> ok = file:close(DataFile),
                                                         lager:warning("Message: Not enough data - read ~p of ~p bytes",[byte_size(Data),Size]),
                                                         FoundData
                                            end;
                eof -> ok = file:close(DataFile),
                       lager:warning("EOF: Not enough data - attempted to read ~p bytes",[Size]),
                       FoundData
            end;
        _ -> ok = file:close(DataFile), 
            FoundData
    end.



simple_query(StreamID,From,To) ->
    File = cake_stream_manager:stream_filename(StreamID),
    Offset = get_indexed_offset(StreamID,From),
    lager:debug("Offset from get_indexed_offset: ~p", [Offset]),
    DataFile = open_data_file(File),
    {ok,Offset} = file:position(DataFile,Offset),
    simple_query(DataFile,[],From,To).


simple_query(DataFile,FoundData,From,To) ->
    case file:read(DataFile,12) of
        {ok,<<TS:64/native-integer,Size:32/native-integer>>} ->
            case file:read(DataFile,Size) of
                {ok,Data} -> case Size == byte_size(Data) of
                                                true -> case (TS >= From) and (TS =< To) of
                                                            true -> simple_query(DataFile,[<<TS:64/big-integer,Size:32/big-integer,Data/binary>>|FoundData],From);
                                                            false -> case TS < To of 
                                                                        true -> simple_query(DataFile,FoundData,From);
                                                                        false -> ok = file:close(DataFile),
                                                                                 list_to_binary(lists:flatten(lists:reverse(FoundData)))
                                                                    end
                                                        end;
                                                false -> ok = file:close(DataFile),
                                                         lager:warning("Message: Not enough data - read ~p of ~p bytes",[byte_size(Data),Size]),
                                                         list_to_binary(lists:flatten(lists:reverse(FoundData)))
                                            end;
                eof -> ok = file:close(DataFile),
                       lager:warning("EOF: Not enough data - attempted to read ~p bytes",[Size]),
                       list_to_binary(lists:flatten(lists:reverse(FoundData)))
            end;
        _ -> ok = file:close(DataFile), 
            list_to_binary(lists:flatten(lists:reverse(FoundData)))
    end.







all_since_query(StreamID,From) ->
    File = cake_stream_manager:stream_filename(StreamID),
    Offset = get_indexed_offset(StreamID,From),
    lager:debug("Offset from get_indexed_offset: ~p", [Offset]),
    DataFile = open_data_file(File),
    {ok,Offset} = file:position(DataFile,Offset),
    all_since_query(DataFile,[],From).


all_since_query(DataFile,FoundData,From) ->
    case file:read(DataFile,12) of
        {ok,<<TS:64/native-integer,Size:32/native-integer>>} ->
            case file:read(DataFile,Size) of
                {ok,Data} -> case Size == byte_size(Data) of
                                                true -> case TS > From of
                                                            true -> all_since_query(DataFile,[<<TS:64/big-integer,Size:32/big-integer,Data/binary>>|FoundData],From);
                                                            false -> all_since_query(DataFile,FoundData,From)
                                                        end;
                                                false -> ok = file:close(DataFile),
                                                         lager:warning("Message: Not enough data - read ~p of ~p bytes",[byte_size(Data),Size]),
                                                         list_to_binary(lists:flatten(lists:reverse(FoundData)))
                                            end;
                eof -> ok = file:close(DataFile),
                       lager:warning("EOF: Not enough data - attempted to read ~p bytes",[Size]),
                       list_to_binary(lists:flatten(lists:reverse(FoundData)))
            end;
        _ -> ok = file:close(DataFile), 
            list_to_binary(lists:flatten(lists:reverse(FoundData)))
    end.
















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
    File = cake_stream_manager:stream_filename(StreamID),
    {ok,DataDir} = application:get_env(cakedb,data_dir),
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







