-module(cake_file_tools).
-compile([{parse_transform, lager_transform}]).

-export([validate_file/1]).



-define(MESSAGE_HEADER,<<1,1,1,TS:64/native-integer,Size:32/native-integer,CRC32:32/native-integer,2,2,2>>).
-define(MESSAGE,<<Data:Size/binary,3,3,3>>).

validate_file(FileName) ->
    {ok,DataFile} = file:open(FileName,[binary,raw,read,read_ahead]),
    validate(DataFile).

validate(DataFile) ->
    case file:read(DataFile,22) of
    {ok,?MESSAGE_HEADER} ->
        %io:format("~p  ~p  ~p~n",[TS,Size,CRC32]),
        case file:read(DataFile,Size+3) of
            {ok,?MESSAGE} -> case CRC32 == erlang:crc32(Data) of
                                            true -> validate(DataFile);
                                            false -> ok = file:close(DataFile),
                                                     lager:warning("Message: CRC32 Checksum failed on ~p",[TS])
                                            end;
            eof -> ok = file:close(DataFile),
                   lager:warning("EOF: Not enough data - attempted to read ~p bytes",[Size])
        end;
    _ -> ok = file:close(DataFile)
end.
