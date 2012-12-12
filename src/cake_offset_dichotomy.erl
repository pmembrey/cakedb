%% ===================================================================
%% This module allows cakedb to use dichotomic search
%% When searching the index file
%% ===================================================================

-module(cake_offset_dichotomy).

-export([get_indexed_offset/2, test/1]).

-include_lib("kernel/include/file.hrl").

-compile([{parse_transform, lager_transform}]).
-compile({inline, [write/4]}).

% A record entry in the index file
-record(record, {
	           timestamp,
	           type,
	           position
	        }
       ).

% A representation of the record
-define(RECORD_REPRESENTATION, <<
				 Timestamp:64/native-integer,
				 Type:8/native-integer,
				 Position:64/native-integer
			       >>
       ).

% Size of a record
% 64 bits + 8 bits + 64 bits
-define(SIZE_OF_RECORD, (8 + 1 + 8)).

%
% Exported function to retrieve an offset from the index file using dichotomy
%
get_indexed_offset(StreamID,From) ->
    {ok, Iodev, Filename} = get_file(StreamID, [binary, read, read_ahead]),
    {ok, #file_info{size = Size}} = file:read_file_info(Filename),
    Nb_records = Size div ?SIZE_OF_RECORD,
    lager:debug("Index file has ~p records~n", [Nb_records]),
    % Check if index file has records to start dichotomy
    if
	Nb_records == 0 ->
	    Position = 0;
	true ->
	    {ok, Position} = start_dichotomy(Iodev, Nb_records - 1, From)
    end,
    file:close(Iodev),
    Position
    .

%
% Initialise the dichotomic process
%
start_dichotomy(Iodev, Back, From) ->
    lager:debug("Starting dichotomy | Back : ~p | From : ~p~n", [Back, From]),
    {ok, Front_record} = getRecord(Iodev, 0),
    {ok, Back_record} = getRecord(Iodev, Back),

    TS_front = Front_record#record.timestamp,
    POS_front = Front_record#record.position,
    TS_back = Back_record#record.timestamp,
    POS_back = Back_record#record.position,

    lager:debug("Left edge timestamp: ~p~n", [TS_front]),
    lager:debug("Right edge timestamp: ~p~n", [TS_back]),

    % Check edges before starting the search
    % We don't want to run a whole search if the answer can be quick to find
    % Or if there are no answers
    if
%	TS_back < From -> {error, "No record"};
	TS_back =< From -> {ok, POS_back};
	TS_front == From -> {ok, POS_front};
	TS_front > From -> {ok, 0};
        % Edges aren't the answer, starting the dichotomic search
	true -> findRecord(Iodev, 0, Back, From)
    end
    .

%
% Retrieve the index file to start processing it
%
get_file(StreamID, Opts) ->
    {ok,File} = cake_stream_manager:stream_filename(StreamID),
    {ok,DataDir} = application:get_env(cake,data_dir),
    Filename = DataDir ++ File ++ "/" ++ File ++ ".index",
    lager:debug("opening file ~s~n", [Filename]),
    {ok, Iodev} = file:open(Filename, Opts),
    {ok, Iodev, Filename}
    .

%
% Retrieve a record from the index file
%
getRecord(Iodev, Offset) ->
    {ok, _} = file:position(Iodev, {bof, (Offset * ?SIZE_OF_RECORD)}),
    {ok, Record} = file:read(Iodev, ?SIZE_OF_RECORD),
    % Macro sets Timestamp, Type and Position
    ?RECORD_REPRESENTATION = Record,
    {ok, #record{timestamp=Timestamp, type=Type, position=Position}}
    .

%
% Search for the record closest to From (record's TS >= From)
% This function ends the recursion
% Search has been narrowed to only one record
% Early edge check confirmed record exist
%
findRecord(Iodev, Front, Back, _From) when Front == Back ->
    lager:debug("[Front == Back] Found record, ~p~n", [Front]),
    {ok, Record} = getRecord(Iodev, Front),
    Position = Record#record.position,
    {ok, Position}
    ;

%
% Search for the record closest to From (record's TS >= From)
% This function ends the recursion
% Search has been narrowed to only two records
% Early edge check confirmed record exist
%
findRecord(Iodev, Front, Back, From) when Front == Back - 1 ->
    lager:debug("[Front == Back - 1] Found record, either ~p or ~p~n", [Front, Back]),
    {ok, Front_record} = getRecord(Iodev, Front),
    {ok, Back_record} = getRecord(Iodev, Back),

    if
	Front_record#record.timestamp == From ->
	                                {ok, Front_record#record.position}
		                        ;
	true -> {ok, Back_record#record.position}
    end
    ;

%
% Search for the record closest to From (record's TS >= From)
%
findRecord(Iodev, Front, Back, From) ->
    % Get the middle record
    Half = (Front + Back) div 2,
    {ok, Record} = getRecord(Iodev, Half),

    TS = Record#record.timestamp,
    Position = Record#record.position,
    if
	% Throw left side of tree
        TS < From -> findRecord(Iodev, Half + 1, Back, From);
	% Throw right side of tree
	TS > From -> findRecord(Iodev, Front, Half - 1, From);
	% Bingo
	true -> {ok, Position}
    end
    .

%% ===================================================================
%% Module test
%% ===================================================================

% Number of record to write to index test file
-define(TEST_LENGTH, 10000).

%
% Run a small test to check the quality of the code 
%
test(StreamId) ->
    {ok, Iodev, Filename} = get_file(StreamId, [binary, write]),
    lager:debug("Filename: ~s~n", [Filename]),

    % Write records in index test file
    writes(Iodev, 0, 0, 0),
    file:close(Iodev),

    % Test every value
    reads(StreamId, 0)
    .

%
% Use dichotimic search with value from
% If test is ok run it with next index
%
reads(StreamId, From) when From < ?TEST_LENGTH ->
    ok = reads_util(StreamId, From),
    reads(StreamId, From + 1)
    ;

%
% Use dichotimic search with value from
% Ends the recursion
%
reads(StreamId, From) ->
    ok = reads_util(StreamId, From)
    .

%
% Get index and check result
%
reads_util(StreamId, From) ->
    % Test dichotomic search
    Pos = get_indexed_offset(StreamId, From),
    lager:debug("Got pos: ~10B with from: ~10B~n", [Pos, From]),
    % Check result
    case Pos == From * 3 of
	true -> ok;
	false -> error
    end
    .

%
% Write an entry to the test index file
% Write TEST_LENGTH entries using recursion
%
writes(Iodev, TS, Type, Pos) when TS < ?TEST_LENGTH ->
    lager:debug("TS: ~10B Type: ~10B Pos: ~10B~n", [TS, Type, Pos]),
    write(Iodev, TS, Type, Pos),
    writes(Iodev, TS + 1, Type, Pos + 3)
    ;

%
% Write an entry to the test index file
%
writes(Iodev, TS, Type, Pos) ->
    write(Iodev, TS, Type, Pos)
    .

%
% Do the IO operation to write the record
%
write(Iodev, Timestamp, Type, Position) ->
    Data = ?RECORD_REPRESENTATION,
    file:write(Iodev, Data)
    .
