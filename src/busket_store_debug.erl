-module(busket_store_debug).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([init/0, record/6, get_series/4, get_events/0, cleanup/2]).

init() ->
    io:format("[store:init]~n").

record(Timestamp, Event, Avg, Min, Max, Resolution) ->
    io:format("[store:record] ts=~p event=~p avg=~p min=~p max=~p resolution=~p~n", [Timestamp, Event, Avg, Min, Max, Resolution]).

get_series(Event, StartTS, EndTS, Resolution) ->
    io:format("[store:get_series] event=~p startts=~p endts=~p resolution=~p~n", [Event, StartTS, EndTS, Resolution]),
    [].

get_events() ->
    io:format("[store:get_events]~n"),
    [].

cleanup(Resolution, Limit) ->
    io:format("[store:cleanup] resolution=~p limit=~p~n", [Resolution, Limit]).
