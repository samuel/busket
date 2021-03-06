-module(busket_store).
-author('Samuel Stauffer <samuel@descolada.com>').

-behaviour(gen_server).

-export([start_link/1, start/1]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_info/2, handle_cast/2]).
%% public
-export([record/8, get_series/4, get_series_info/1, get_events/1,
    get_last_update_time/1, set_last_update_time/2, cleanup/2]).

-record(state, {module, modstate}).

start_link(StoreModule) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {self(), StoreModule}, []).

start(StoreModule) ->
    gen_server:start({local, ?MODULE}, ?MODULE, {self(), StoreModule}, []).

record(Timestamp, Event, Avg, Min, Max, Variance, Resolution, MainInterval) ->
	gen_server:call(?MODULE, {record, Timestamp, Event, Avg, Min, Max, Variance, Resolution, MainInterval}).

get_series(Event, StartTS, EndTS, Resolution) ->
	gen_server:call(?MODULE, {get_series, Event, StartTS, EndTS, Resolution}).

get_series_info(Resolution) ->
    gen_server:call(?MODULE, {get_series_info, Resolution}).

get_events(Since) ->
	gen_server:call(?MODULE, {get_events, Since}).

set_last_update_time(Resolution, Timestamp) ->
    gen_server:call(?MODULE, {set_last_update_time, Resolution, Timestamp}).

get_last_update_time(Resolution) ->
    gen_server:call(?MODULE, {get_last_update_time, Resolution}).

cleanup(Resolution, Limit) ->
    gen_server:call(?MODULE, {cleanup, Resolution, Limit}).

%% gen_server callbacks
init({_PidMaster, Module}) ->
    ModState = Module:init(),
    {ok, #state{module=Module, modstate=ModState}}.

handle_call({record, Timestamp, Event, Avg, Min, Max, Variance, Resolution, MainInterval}, _From, #state{module=Module, modstate=ModState} = State) ->
    ModState2 = Module:record(ModState, Timestamp, Event, Avg, Min, Max, Variance, Resolution, MainInterval),
    {reply, ok, State#state{modstate=ModState2}};
handle_call({get_series, Event, StartTS, EndTS, Resolution}, _From, #state{module=Module, modstate=ModState} = State) ->
    {ModState2, Res} = Module:get_series(ModState, Event, StartTS, EndTS, Resolution),
    {reply, Res, State#state{modstate=ModState2}};
handle_call({get_series_info, Resolution}, _From, #state{module=Module, modstate=ModState} = State) ->
    {ModState2, Res} = Module:get_series_info(ModState, Resolution),
    {reply, Res, State#state{modstate=ModState2}};
handle_call({get_events, Since}, _From, #state{module=Module, modstate=ModState} = State) ->
    {ModState2, Res} = Module:get_events(ModState, Since),
    {reply, Res, State#state{modstate=ModState2}};
handle_call({get_last_update_time, Resolution}, _From, #state{module=Module, modstate=ModState} = State) ->
    {ModState2, Res} = Module:get_last_update_time(ModState, Resolution),
    {reply, Res, State#state{modstate=ModState2}};
handle_call({set_last_update_time, Resolution, Timestamp}, _From, #state{module=Module, modstate=ModState} = State) ->
    ModState2 = Module:set_last_update_time(ModState, Resolution, Timestamp),
    {reply, ok, State#state{modstate=ModState2}};
handle_call({cleanup, Resolution, Limit}, _From, #state{module=Module, modstate=ModState} = State) ->
    ModState2 = Module:cleanup(ModState, Resolution, Limit),
    {reply, ok, State#state{modstate=ModState2}};
handle_call(Call, _From, State) ->
    io:format("UNANDLED handle_call ~p ~p~n", [Call, State]),
    {reply, ok, State}.

handle_info({record, Timestamp, Event, Avg, Min, Max, Variance, Resolution}, #state{module=Module, modstate=ModState} = State) ->
    ModState2 = Module:record(ModState, Timestamp, Event, Avg, Min, Max, Variance, Resolution),
    {noreply, State#state{modstate=ModState2}};
handle_info(Info, State) ->
    io:format("UNHANDLED handle_info ~p ~p~n", [Info, State]),
    {noreply, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

terminate(Reason, _State) ->
    io:format("~p stopping: ~p~n", [?MODULE, Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
