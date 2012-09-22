-module(cake_query).
-compile([{parse_transform, lager_transform}]).

-export([return_all/1,all_since_query/2,dump_index/1]).


return_all(FileName) ->
	{ok,Data} = file:read_file(FileName),
	print_all(Data,0).


print_all(<<>>,Count) ->
	io:format("Total records: ~p~n",[Count]),
	ok;
print_all(Data,Count) ->
	<<TS:64/big-integer,Size:32/big-integer,Compressed:Size/binary,TheRest/binary>> = Data,
	{ok,Decompressed} = snappy:decompress(Compressed),
	io:format("~p|~p|~p~n",[TS,Size,Decompressed]),
	print_all(TheRest,Count + 1).



all_since_query(StreamID,From) ->
    File = cake_stream_manager:stream_filename(StreamID),
    Offset = get_indexed_offset(StreamID,From),
    lager:debug("Offset from get_indexed_offset: ~p", [Offset]),
    {ok,DataFile} = file:open("data/" ++ File ++ "/" ++ File ++ ".data",[binary,raw,read,read_ahead]),
    {ok,Offset} = file:position(DataFile,Offset),
    all_since_query(DataFile,[],From).


all_since_query(DataFile,FoundData,From) ->
    case file:read(DataFile,12) of
        {ok,<<TS:64/big-integer,Size:32/big-integer>>} ->
            case file:read(DataFile,Size) of
                {ok,CompressedData} -> case Size == byte_size(CompressedData) of
                                                true -> case TS > From of
                                                            true -> {ok,Decompressed} = snappy:decompress(CompressedData),
                                                                DecompressedSize = byte_size(Decompressed),
                                                                all_since_query(DataFile,[<<TS:64/big-integer,DecompressedSize:32/big-integer,Decompressed/binary>>|FoundData],From);
                                                            false -> all_since_query(DataFile,FoundData,From)
                                                        end;
                                                false -> all_since_query(DataFile,FoundData,From)
                                            end;
                eof -> all_since_query(DataFile,FoundData,From)  % Not convinced that this is right...
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
    <<TS:64/big-integer,Type:8/big-integer,ByteLocation:64/big-integer,TheRest/binary>> = Data,
    
    io:format("~p|~p|~p~n",[TS,Type,ByteLocation]),
    dump_index(TheRest,Count + 1).



get_indexed_offset(StreamID,From) ->
    File = cake_stream_manager:stream_filename(StreamID),
    {ok,Data} = file:read_file("data/" ++ File ++ "/" ++ File ++ ".index"),
    get_indexed_offset(Data,From,0).

get_indexed_offset(<<>>,_From,Offset) ->
    Offset;
get_indexed_offset(Data,From,Offset) ->
    <<TS:64/big-integer,_Type:8/big-integer,ByteLocation:64/big-integer,TheRest/binary>> = Data,
    case From > TS of
        true -> get_indexed_offset(TheRest,From,ByteLocation);
        false -> Offset
    end.







