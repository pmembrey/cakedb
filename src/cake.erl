-module(cake).
-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).


-export([init/1,start_link/0,
	     terminate/2,handle_call/3,handle_info/2,
	     code_change/3,handle_cast/2,
	     stop/0]).






start_link() ->
	gen_server:start_link({local,?MODULE},?MODULE,[],[]).


init(_) ->

    %Move this lot to configuration file later
    application:start(gproc),
    application:start(compiler),
    application:start(syntax_tools),
    application:start(goldrush),
    application:start(lager),
    lager:set_loglevel(lager_console_backend, debug),    
    lager:set_loglevel(lager_file_backend, "log/console.log", info),


    % StatsD Start up
    %application:start(statsderl),
    %application:start(vmstats),

    % TCP Ranch
    application:start(ranch),
    ranch:start_listener(cake_protocol_listener,100,ranch_tcp, [{port,8888}],cake_protocol,[]),


    process_flag(trap_exit, true),


    application:start(folsom),
    application:start(cowboy),
    application:start(folsom_cowboy),


    folsom_metrics:new_meter(<<"msg_per_sec">>),


    {ok,DataDir} = application:get_env(cake,data_dir),

    case filelib:ensure_dir(DataDir) of
        {error,Reason}  -> lager:error("Cannot open '~p': ~p",[DataDir,Reason]);
        ok              -> cake_stream_manager:start_link(),
                           lager:info("CakeDB Ready."),
                           {ok,{}}
    end.












%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


terminate(_Reason,_State) ->
	ok.


handle_cast(stop,State) ->
	{stop,normal,State}.


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
	gen_server:cast(cake,stop).
