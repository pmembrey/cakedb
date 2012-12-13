-module(cake_stream_manager).
-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).


-export([init/1,start_link/0,
	     terminate/2,handle_call/3,handle_info/2,
	     code_change/3,handle_cast/2,
	     stop/0]).

-export([register_stream/1,stream_filename/1,valid_stream_name/1]).

-record(state,{streams,counter}).




start_link() ->
	gen_server:start_link({local,?MODULE},?MODULE,[],[]).


init(_) ->

	%Move this lot to configuration file later
    lager:set_loglevel(lager_console_backend, info),    
    lager:set_loglevel(lager_file_backend, "log/console.log", info),

    process_flag(trap_exit, true),

    State = #state{ streams = [],
    				counter =0
                   },


   
    lager:info("Cake Stream Manager Ready."),
	{ok,State}.





register_stream(StreamName) ->
	gen_server:call(?MODULE,{register,StreamName}).

stream_filename(StreamID) ->
    gen_server:call(?MODULE,{filename,StreamID}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%





terminate(_Reason,_State) ->
	ok.


handle_cast(stop,State) ->
	{stop,normal,State}.


handle_call({register,Stream},_From,State) ->	

    lager:debug("Stream state data: ~p",[(State#state.streams)]),

    case valid_stream_name(Stream) of
        ok ->
	       case proplists:is_defined(Stream,State#state.streams) of
		      true ->
			     {StreamID,_FileName} = proplists:get_value(Stream,State#state.streams),
			     {reply,StreamID,State};
		      false ->
			     StreamID = State#state.counter + 1,
			     lager:info("Allocating ID ~p to stream ~p",[StreamID,Stream]),
                 FileName = binary_to_list(Stream),
			     spawn_link(cake_stream,init,[Stream,StreamID,FileName]),
			     gproc:await({n,l,{stream,StreamID}}),
			     {reply,StreamID,State#state{counter=StreamID,streams=   lists:flatten([[{Stream,{StreamID,FileName}},{StreamID,{Stream,FileName}}]|State#state.streams] )}}
	        end;
        notok -> lager:warning("Attempt to use an invalid stream name detected."),
                 {reply,badstream,State}
    end;

handle_call({filename,StreamID},_From,State) ->
    lager:debug("StreamID as passed to filename looker upper: ~p",[StreamID]),
    case proplists:is_defined(StreamID,State#state.streams) of
        true ->
            {_Stream,FileName} = proplists:get_value(StreamID,State#state.streams),
            {reply,{ok,FileName},State};
        false ->
            {reply,unregistered_stream,State}
    end;

handle_call(terminate,_From,State) ->
	{stop,normal,ok,State}.




handle_info(Msg,State) ->
	io:format("Unknown message: ~p~n",[Msg]),
	{noreply,State}.


code_change(_OldVsn, State, _Extra) ->
	%% No change planned. The function is there for the behaviour,
	%% but will not be used. Only a version on the next
	{ok, State}.


stop() ->
	gen_server:cast(cake_stream_manager,stop).



valid_stream_name(StreamName) ->
    case re:run(StreamName,"\\A[a-zA-Z0-9\\-_]+\\z") of
        {match,_}  -> ok;
        _          -> notok
    end.

