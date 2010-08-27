-module(busket_store_mongo).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([
    init/0, record/6, get_series/4, get_series_info/1,
    get_last_update_time/1, set_last_update_time/2, get_events/0, cleanup/2]).

init() ->
    ok.

record(Timestamp, Event, Avg, Min, Max, Resolution) ->
    CollectionName = collection_name(Resolution),
    emongo:insert(mongo_busket, CollectionName, [
        {"ts", Timestamp},
        {"event", Event},
        {"avg", Avg},
        {"min", Min},
        {"max", Max}
    ]),
    % io:format("MONGO ~p ~p ~p ~p ~p ~p~n", [Timestamp, Event, Avg, Min, Max, Resolution]),
    emongo:update(mongo_busket, "events", [{"name", Event}], [{"$set", [{"last_seen", Timestamp}, {"last_value", Avg}]}], true).

get_series(Event, StartTS, EndTS, Resolution) ->
    CollectionName = collection_name(Resolution),
    emongo:find(mongo_busket, CollectionName, [{"event", Event}, {"ts", [{gte, StartTS}, {lt, EndTS}]}], [{orderby, {"ts", asc}}]).

get_events() ->
    emongo:find(mongo_busket, "events").

get_series_info(Resolution) ->
    case emongo:find(mongo_busket, "series", [{"resolution", Resolution}], [{limit, 1}]) of
        [Info] -> Info;
        [] -> []
    end.

get_last_update_time(Resolution) ->
    proplists:get_value(<<"last_update">>, get_series_info(Resolution), 0).

set_last_update_time(Resolution, Timestamp) ->
    emongo:update(mongo_busket, "series", [{"resolution", Resolution}], [{"$set", [{"last_update", Timestamp}]}], true).

cleanup(Resolution, Limit) ->
    CollectionName = collection_name(Resolution),
    emongo:delete(mongo_busket, CollectionName, [{"ts", [{lt, busket:get_unix_timestamp() - Limit*Resolution}]}]).

collection_name(Resolution) ->
    string:concat("series_", integer_to_list(Resolution)).
