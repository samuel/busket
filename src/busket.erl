-module(busket).
-author('Samuel Stauffer <samuel@descolada.com>').

-behaviour(gen_server).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_info/2, handle_cast/2]).
% public
-export([record/3, record/1, cleanup/0, rollup/0, get_unix_timestamp/0, get_unix_timestamp/1]).

-define(ABSOLUTE_TYPE, 97).
-define(COUNTER_TYPE, 99).
-define(GAUGE_TYPE, 103).
-define(CLEANUP_INTERVAL, 30*60*1000). % ms
-define(DEFAULT_INTERVAL, 60000). % ms
-define(INTERVALS, [
    {60, 1440},      % Every minute for 24 hours
    {5*60, 576},     % Every 5 minutes for 48 hours
    {30*60, 432},    % Every 30 minutes for 9 days
    {60*60, 1080},   % Every 1 hours for 45 days
    {24*60*60, 450}  % Every day for 450 days
]).

-record(state, {last_ts, events, last_values, store}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {self()}, []).

record(Type, Event, Value) ->
    record([{Type, Event, Value}]).

record(Events) ->
    gen_server:cast(?MODULE, {record, Events}).

%% gen_server callbacks

init({_PidMaster}) ->
	process_flag(trap_exit, true),
    timer:start(),
    {ok, _} = timer:apply_interval(?CLEANUP_INTERVAL, ?MODULE, cleanup, []),
    {ok, _} = timer:apply_interval(element(1, lists:nth(2, ?INTERVALS)), ?MODULE, rollup, []),
    timer:send_after(time_to_next_interval(?DEFAULT_INTERVAL), collection_timer),
    {ok, #state{
            last_ts = erlang:now(),
            events = dict:new(),
            last_values = dict:new()
        }}.

handle_call(Call, _From, State) ->
    io:format("UNANDLED handle_call ~p ~p~n", [Call, State]),
    {reply, ok, State}.

handle_info(collection_timer, State) ->
    NextTS = erlang:now(),
    Interval = timer:now_diff(NextTS, State#state.last_ts) / 1000000,
    {NewState1, _, _} = dict:fold(fun process_events/3, {State, Interval, NextTS}, State#state.events),
    NewState2 = NewState1#state{events=dict:new(), last_ts=NextTS},
    timer:send_after(time_to_next_interval(?DEFAULT_INTERVAL), collection_timer),
    {noreply, NewState2};
% handle_info(aggregate_timer, State) ->
%     {noreply, State};
handle_info(Info, State) ->
    io:format("UNHANDLED handle_info ~p ~p~n", [Info, State]),
    {noreply, State}.

handle_cast({record, NewEvents}, State) ->
    NewState = record_events(NewEvents, State),
    {noreply, NewState};
handle_cast(stop, State) ->
    {stop, normal, State}.

terminate(Reason, _State) ->
    io:format("~p stopping: ~p~n", [?MODULE, Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%

record_events([], State) ->
    State;
record_events([{?GAUGE_TYPE, Name, Value}|Rest], #state{events=Events} = State) ->
    {Sum, Min, Max, Count} = case dict:find({Name, ?GAUGE_TYPE}, Events) of
        {ok, {OldSum, OldMin, OldMax, OldCount}} ->
            {OldSum+Value, erlang:min(OldMin, Value), erlang:max(OldMax, Value), OldCount+1};
        error ->
            {Value, Value, Value, 1}
    end,
    NewEvents = dict:store({Name, ?GAUGE_TYPE}, {Sum, Min, Max, Count}, Events),
    NewState = State#state{events=NewEvents},
    record_events(Rest, NewState);
record_events([{?COUNTER_TYPE, Name, Value}|Rest], #state{events=Events} = State) ->
    NewEvents = dict:store({Name, ?COUNTER_TYPE}, Value, Events),
    NewState = State#state{events=NewEvents},
    record_events(Rest, NewState);
record_events([{?ABSOLUTE_TYPE, Name, Value}|Rest], #state{events=Events} = State) ->
    NewEvents = dict:update_counter({Name, ?ABSOLUTE_TYPE}, Value, Events),
    NewState = State#state{events=NewEvents},
    record_events(Rest, NewState).

process_events({Name, EventType}, Counters, {State, Interval, TS}) ->
    LastValue = case dict:find({Name, EventType}, State#state.last_values) of
        {ok, LV} ->
            LV;
        error ->
            nil
    end,
    {Avg, Min, Max, Value} = aggregate_events(EventType, Counters, Interval, LastValue),
    NewState = State#state{last_values=dict:store({Name, EventType}, Value, State#state.last_values)},
    case Avg of
        nil ->
            ok;
        _ ->
            busket_store:record(get_unix_timestamp(TS), Name, Avg, Min, Max, erlang:round(?DEFAULT_INTERVAL/1000))
    end,
    {NewState, Interval, TS}.

aggregate_events(?ABSOLUTE_TYPE, Sum, Interval, _LastValue) ->
    Avg = Sum/Interval,
    {Avg, Avg, Avg, Sum};
aggregate_events(?COUNTER_TYPE, Value, _Interval, nil) ->
    {nil, nil, nil, Value};
aggregate_events(?COUNTER_TYPE, Value, Interval, LastValue) ->
    Avg = (Value-LastValue)/Interval,
    {Avg, Avg, Avg, Value};
aggregate_events(?GAUGE_TYPE, {Sum, Min, Max, Count}, _Interval, _LastValue) ->
    {Sum/Count, Min, Max, Sum}.

cleanup() ->
    cleanup(?INTERVALS).
cleanup([]) ->
    ok;
cleanup([{Resolution, Limit}|Intervals]) ->
    busket_store:cleanup(Resolution, Limit),
    cleanup(Intervals).

rollup() ->
    [{Resolution, Limit}|Intervals] = ?INTERVALS,
    rollup(Intervals, {Resolution, Limit}).
rollup([], _) ->
    ok;
rollup([{Resolution, Limit}|Intervals], {LastResolution, _}) ->
    LastUpdate = busket_store:get_last_update_time(Resolution),
    LastStep = LastUpdate div Resolution,
    CurrentStep = get_unix_timestamp() div Resolution,
    if 
        CurrentStep > LastStep ->
            StartTS = CurrentStep * Resolution,
            EndTS = StartTS + Limit,
            Events = [proplists:get_value(<<"name">>, E) || E <- busket_store:get_events(CurrentStep * Resolution)],
            rollup_aggregate(Events, StartTS, EndTS, Resolution, LastResolution);
        true ->
            ok
    end,
    % TODO
    % busket_store:last_
    rollup(Intervals, {Resolution, Limit}).

rollup_aggregate([], _, _, _, _) ->
    ok;
rollup_aggregate([Event|Events], StartTS, EndTS, Resolution, LastResolution) ->
    Series = busket_store:get_series(Event, StartTS, EndTS, LastResolution),
    {Sum, Count, Min, Max} = aggregate(Series),
    Average = Sum / Count,
    busket_store:record(EndTS, Event, Average, Min, Max, Resolution),
    rollup_aggregate(Events, StartTS, EndTS, Resolution, LastResolution).

aggregate(Series) ->
    aggregate(Series, 0, 0, null, null).
aggregate([], Sum, Count, Min, Max) ->
    {Sum, Count, Min, Max};
aggregate([Event|Series], Sum, Count, Min, Max) ->
    Sum2 = Sum + proplists:get_value(<<"avg">>, Event),
    Min2 = erlang:min(Min, proplists:get_value(<<"min">>, Event)),
    OldMax = proplists:get_value(<<"max">>, Event),
    Max2 = if
        Max == null ->
            OldMax;
        true ->
            erlang:min(Max, proplists:get_value(<<"max">>, Event))
    end,
    Count2 = Count + 1,
    aggregate(Series, Sum2, Count2, Min2, Max2).

time_to_next_interval(Interval) ->
    {Megaseconds, Seconds, Microseconds} = erlang:now(),
    MS = Interval - ((timer:seconds(Megaseconds*1000000+Seconds)+Microseconds div 1000) rem Interval),
    MS2 = case MS < 1000 of
        true -> MS + Interval;
        false -> MS
    end,
    MS2.

get_unix_timestamp() ->
    get_unix_timestamp(erlang:now()).
get_unix_timestamp(TS) ->
    calendar:datetime_to_gregorian_seconds(calendar:now_to_universal_time(TS)) -
        calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}).
