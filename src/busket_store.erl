-module(busket_store).
-author('Samuel Stauffer <samuel@descolada.com>').

-behaviour(gen_server).

-export([start_link/1, start/1]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_info/2, handle_cast/2]).
%% public
-export([record/6, get_series/4, get_events/0, cleanup/2]).

-record(state, {module}).

start_link(StoreModule) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {self(), StoreModule}, []).

start(StoreModule) ->
    gen_server:start({local, ?MODULE}, ?MODULE, {self(), StoreModule}, []).

record(Timestamp, Event, Avg, Min, Max, Resolution) ->
	gen_server:call(?MODULE, {record, Timestamp, Event, Avg, Min, Max, Resolution}, infinity).

get_series(Event, StartTS, EndTS, Resolution) ->
	gen_server:call(?MODULE, {get_series, Event, StartTS, EndTS, Resolution}, infinity).

get_events() ->
	gen_server:call(?MODULE, get_events, infinity).

cleanup(Resolution, Limit) ->
    gen_server:call(?MODULE, {cleanup, Resolution, Limit}).

%% gen_server callbacks
init({_PidMaster, Module}) ->
    Module:init(),
    {ok, #state{module=Module}}.

handle_call({record, Timestamp, Event, Avg, Min, Max, Resolution}, _From, #state{module=Module} = State) ->
    Module:record(Timestamp, Event, Avg, Min, Max, Resolution),
    {reply, ok, State};
handle_call({get_series, Event, StartTS, EndTS, Resolution}, _From, #state{module=Module} = State) ->
    Res = Module:get_series(Event, StartTS, EndTS, Resolution),
    {reply, Res, State};
handle_call(get_events, _From, #state{module=Module} = State) ->
    Res = Module:get_events(),
    {reply, Res, State};
handle_call({cleanup, Resolution, Limit}, _From, #state{module=Module} = State) ->
    Module:cleanup(Resolution, Limit),
    {noreply, ok, State};
handle_call(Call, _From, State) ->
    io:format("UNANDLED handle_call ~p ~p~n", [Call, State]),
    {reply, ok, State}.

handle_info({record, Timestamp, Event, Avg, Min, Max, Resolution}, #state{module=Module} = State) ->
    Module:record(Timestamp, Event, Avg, Min, Max, Resolution),
    {noreply, State};
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