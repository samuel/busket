-module(busket_store_mongo).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([
    init/0, record/10, get_series/5, get_series_info/2,
    get_last_update_time/2, set_last_update_time/3, get_events/2, cleanup/3]).

init() ->
    ok.

record(State, Timestamp, Event, Sum, Count, Min, Max, Variance, Resolution, MainInterval) ->
    CollectionName = collection_name(Resolution),
    emongo:update(mongo_busket, CollectionName, [
            {"ts", Timestamp},
            {"event", Event}
        ], [
            "$inc", [
                {"sum", Sum},
                {"count", Count},
                {"min", Min},
                {"max", Max},
                {"variance", if Variance == nil -> 0; true -> Variance end}
            ]
        ], true),
    % io:format("MONGO ~p ~p ~p ~p ~p ~p ~p~n", [Timestamp, Event, Avg, Min, Max, Variance, Resolution]),
    if
        MainInterval == true ->
            emongo:update(mongo_busket, "events",
                [{"name", Event}],
                [
                    {"$set", [
                        {"last_seen", Timestamp}
                    ]},
                    {"$add", [
                        {"sum", Sum},
                        {"count", Count}
                    ]}
                ], true);
        true ->
            ok
    end,        
    State.

get_series(State, Event, StartTS, EndTS, Resolution) ->
    CollectionName = collection_name(Resolution),
    Res = emongo:find(mongo_busket, CollectionName, [{"event", Event}, {"ts", [{gte, StartTS}, {lt, EndTS}]}], [{orderby, [{"ts", asc}]}]),
    {State, Res}.

get_events(State, Since) ->
    Res = emongo:find(mongo_busket, "events", [{"last_seen", [{gte, Since}]}]),
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
