-module(busket_store_mongo).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([
    init/0, record/7, get_series/5, get_series_info/2,
    get_last_update_time/2, set_last_update_time/3, get_events/2, cleanup/3]).

init() ->
    ok.

record(State, Timestamp, Event, Avg, Min, Max, Resolution) ->
    CollectionName = collection_name(Resolution),
    emongo:insert(mongo_busket, CollectionName, [
        {"ts", Timestamp},
        {"event", Event},
        {"avg", Avg},
        {"min", Min},
        {"max", Max}
    ]),
    % io:format("MONGO ~p ~p ~p ~p ~p ~p~n", [Timestamp, Event, Avg, Min, Max, Resolution]),
    emongo:update(mongo_busket, "events", [{"name", Event}], [{"$set", [{"last_seen", Timestamp}, {"last_value", Avg}]}], true),
    State.

get_series(State, Event, StartTS, EndTS, Resolution) ->
    CollectionName = collection_name(Resolution),
    Res = emongo:find(mongo_busket, CollectionName, [{"event", Event}, {"ts", [{gte, StartTS}, {lt, EndTS}]}], [{orderby, {"ts", asc}}]),
    {State, Res}.

get_events(State, Since) ->
    Res = emongo:find(mongo_busket, "events", [{"last_update", [{gte, Since}]}]),
    {State, Res}.

get_series_info(State, Resolution) ->
    Res = case emongo:find(mongo_busket, "series", [{"resolution", Resolution}], [{limit, 1}]) of
        [Info] -> Info;
        [] -> []
    end,
    {State, Res}.

get_last_update_time(State, Resolution) ->
    {NewState, Info} = get_series_info(State, Resolution),
    Res = proplists:get_value(<<"last_update">>, Info, 0),
    {NewState, Res}.

set_last_update_time(State, Resolution, Timestamp) ->
    emongo:update(mongo_busket, "series", [{"resolution", Resolution}], [{"$set", [{"last_update", Timestamp}]}], true),
    State.

cleanup(State, Resolution, Limit) ->
    CollectionName = collection_name(Resolution),
    emongo:delete(mongo_busket, CollectionName, [{"ts", [{lt, busket:get_unix_timestamp() - Limit*Resolution}]}]),
    State.

collection_name(Resolution) ->
    string:concat("series_", integer_to_list(Resolution)).
