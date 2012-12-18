-module(proper_utils).

-export([streamname/0,streamid/1,cleanup/1]).

-include_lib("proper/include/proper.hrl").

-define(STREAMNAMES, [<<"tempfile">>, <<"file001">>, <<"anotherfile">>,
        <<"somefile">>, <<"binfile">>, <<"cakestream">>]).

-record(state,{starttime,counter,streams}).

streamname() -> 
    elements(?STREAMNAMES).

% Return any of the existing StreamID
% plus an unassgined one for testing
streamid(#state{counter = Counter}) ->
    elements(lists:seq(1, Counter+1)).

% Cleans the data directory of the files
% for stream StreamName
cleanup(StreamName) ->
    case application:get_env(cake,data_dir) of 
        {ok,DataDir} -> DirName = DataDir++StreamName++"/",
                        file:delete(DirName++StreamName++".index"),
                        file:delete(DirName++StreamName++".data"),
                        file:del_dir(DirName);
        undefined ->    lager:warning("Couldn't delete CakeDB data directory: Couldn't find CakeDB data directory from config file...")
    end,
    ok.

