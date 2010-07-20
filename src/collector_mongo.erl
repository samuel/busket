-module(collector_mongo).
-author('Samuel Stauffer <samuel@descolada.com>').

-behaviour(gen_server).

-export([start_link/0, start/0]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_info/2, handle_cast/2]).
%% puoblic
-export([record/6, get_series/4, get_events/0]).

-record(state, {}).

start_link() ->
    gen_server:start_link(?MODULE, {self()}, []).

start() ->
    gen_server:start(?MODULE, {self()}, []).

record(Timestamp, Event, Avg, Min, Max, Resolution) ->
	gen_server:call(?MODULE, {record, Timestamp, Event, Avg, Min, Max, Resolution}, infinity).

get_series(Event, StartTS, EndTS, Resolution) ->
	gen_server:call(?MODULE, {get_series, Event, StartTS, EndTS, Resolution}, infinity).

get_events() ->
	gen_server:call(?MODULE, get_events, infinity).

%% gen_server callbacks
init({_PidMaster}) ->
    emongo:add_pool(mongo_busket, "localhost", 27017, "busket", 1),
    {ok, #state{}}.

handle_call({record, Timestamp, Event, Avg, Min, Max, Resolution}, _From, State) ->
    private_record(Timestamp, Event, Avg, Min, Max, Resolution),
    {reply, ok, State};
handle_call({get_series, Event, StartTS, EndTS, Resolution}, _From, State) ->
    Res = private_get_series(Event, StartTS, EndTS, Resolution),
    {reply, Res, State};
handle_call(get_events, _From, State) ->
    Res = private_get_events(),
    {reply, Res, State};
handle_call(Call, _From, State) ->
    io:format("UNANDLED handle_call ~p ~p~n", [Call, State]),
    {reply, ok, State}.

handle_info({record, Timestamp, Event, Avg, Min, Max, Resolution}, State) ->
    private_record(Timestamp, Event, Avg, Min, Max, Resolution),
    {noreply, State};
handle_info(Info, State) ->
    io:format("UNHANDLED handle_info ~p ~p~n", [Info, State]),
    {noreply, State}.

terminate(Reason, _State) ->
    io:format("~p stopping: ~p~n", [?MODULE, Reason]),
    ok.

handle_cast(stop, State) ->
    {stop, normal, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%

private_record(Timestamp, Event, Avg, Min, Max, Resolution) ->
    CollectionName = string:concat("series_", integer_to_list(Resolution)),
    emongo:insert(mongo_busket, CollectionName, [
        {"ts", Timestamp},
        {"name", Event},
        {"avg", Avg},
        {"min", Min},
        {"max", Max}
    ]),
    emongo:update(mongo_busket, "events", [{"event", Event}], [{"last_seen", Timestamp}, {"last_value", Avg}], true).
    % io:format("MONGO ~p ~p ~p ~p ~p ~p~n", [Timestamp, Event, Avg, Min, Max, Resolution]).

private_get_series(Event, StartTS, EndTS, Resolution) ->
    CollectionName = string:concat("series_", integer_to_list(Resolution)),
    emongo:find(mongo_busket, CollectionName, [{"event", Event}, {"ts", [{gte, StartTS}, {lte, EndTS}]}], [{orderby, "ts"}]).

private_get_events() ->
    emongo:find(mongo_busket, "events").
