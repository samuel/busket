-module(busket_store_debug).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([init/0, record/7, get_series/5, get_events/2, get_last_update_time/2, cleanup/3]).

init() ->
    % io:format("[store_debug:init]~n"),
    [].

record(State, Timestamp, Event, Avg, Min, Max, Resolution) ->
    % io:format("[stor_debuge:record] ts=~p event=~p avg=~p min=~p max=~p resolution=~p~n", [Timestamp, Event, Avg, Min, Max, Resolution]),
    [{Timestamp, Event, Avg, Min, Max, Resolution}|State].

get_series(State, Event, StartTS, EndTS, Resolution) ->
    % io:format("[store_debug:get_series] event=~p startts=~p endts=~p resolution=~p~n", [Event, StartTS, EndTS, Resolution]),
    {State, []}.

get_events(State, Since) ->
    io:format("[store_debug:get_events] since=~p~n", [Since]),
    {State, []}.

get_last_update_time(State, Resolution) ->
    io:format("[store_debug:get_last_update_time] ~p~n", [Resolution]),
    {State, 0}.

cleanup(State, Resolution, Limit) ->
    io:format("[store_debug:cleanup] resolution=~p limit=~p~n", [Resolution, Limit]),
    State.
