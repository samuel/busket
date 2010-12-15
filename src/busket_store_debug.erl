-module(busket_store_debug).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([init/0, record/8, get_series/5, get_events/2, get_last_update_time/2, cleanup/3]).

-record(event, {ts, name, avg, min, max, resolution}).

init() ->
    % io:format("[store_debug:init]~n"),
    [].

record(State, Timestamp, Event, Avg, Min, Max, Resolution, _MainInterval) ->
    % io:format("[stor_debuge:record] ts=~p event=~p avg=~p min=~p max=~p resolution=~p~n", [Timestamp, Event, Avg, Min, Max, Resolution]),
    [#event{ts=Timestamp, name=Event, avg=Avg, min=Min, max=Max, resolution=Resolution}|State].

get_series(State, Event, StartTS, EndTS, Resolution) ->
    % io:format("[store_debug:get_series] event=~p startts=~p endts=~p resolution=~p~n", [Event, StartTS, EndTS, Resolution]),
    Res = get_series(State, Event, StartTS, EndTS, Resolution, []),
    {State, Res}.

get_series([], _, _, _, _, Series) ->
    Series;
get_series([#event{name=Event, ts=Timestamp, resolution=Resolution, avg=Avg, min=Min, max=Max}|State], Event, StartTS, EndTS, Resolution, Series) ->
    Series2 = if
        (Timestamp >= StartTS) and (Timestamp < EndTS) ->
            [[
                {<<"ts">>, Timestamp},
                {<<"event">>, Event},
                {<<"avg">>, Avg},
                {<<"min">>, Min},
                {<<"max">>, Max}
            ]|Series];
        true ->
            Series
    end,
    get_series(State, Event, StartTS, EndTS, Resolution, Series2);
get_series([_|State], Event, StartTS, EndTS, Resolution, Series) ->
    get_series(State, Event, StartTS, EndTS, Resolution, Series).

get_events(State, Since) ->
    % io:format("[store_debug:get_events] since=~p~n", [Since]),
    Events = sets:to_list(sets:from_list([Event#event.name || Event <- State, Event#event.ts >= Since])),
    {State, Events}.

get_last_update_time(State, Resolution) ->
    % io:format("[store_debug:get_last_update_time] ~p~n", [Resolution]),
    Res = get_last_update_time2(State, Resolution),
    {State, Res}.

get_last_update_time2([], _) ->
    0;
get_last_update_time2([#event{resolution=Resolution, ts=Timestamp}|_], Resolution) ->
    Timestamp;
get_last_update_time2([_|Events], Resolution) ->
    get_last_update_time2(Events, Resolution).

cleanup(State, Resolution, Limit) ->
    io:format("[store_debug:cleanup] resolution=~p limit=~p~n", [Resolution, Limit]),
    State.
