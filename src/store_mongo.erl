-module(store_mongo).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([init/0, record/6, get_series/4, get_events/0, cleanup/2]).

init() ->
    emongo:add_pool(mongo_busket, "localhost", 27017, "busket", 1).

record(Timestamp, Event, Avg, Min, Max, Resolution) ->
    CollectionName = string:concat("series_", integer_to_list(Resolution)),
    emongo:insert(mongo_busket, CollectionName, [
        {"ts", Timestamp},
        {"name", Event},
        {"avg", Avg},
        {"min", Min},
        {"max", Max}
    ]),
    emongo:update(mongo_busket, "events", [{"event", Event}], [{"last_seen", Timestamp}, {"last_value", Avg}], true).

get_series(Event, StartTS, EndTS, Resolution) ->
    CollectionName = string:concat("series_", integer_to_list(Resolution)),
    emongo:find(mongo_busket, CollectionName, [{"event", Event}, {"ts", [{gte, StartTS}, {lte, EndTS}]}], [{orderby, "ts"}]).

get_events() ->
    emongo:find(mongo_busket, "events").

cleanup(_Resolution, _Limit) ->
    io:format("[store_mongo:cleanup] TODO~n").
