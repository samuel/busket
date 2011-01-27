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

-record(state, {last_ts, events, message_count, last_values, store}).

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
    {ok, Intervals} = application:get_env(busket, intervals),
    {ok, _} = timer:apply_interval(?CLEANUP_INTERVAL, ?MODULE, cleanup, []),
    {ok, _} = timer:apply_interval(element(1, lists:nth(2, Intervals))*1000, ?MODULE, rollup, []),
    timer:send_after(time_to_next_interval(?DEFAULT_INTERVAL), collection_timer),
    {ok, #state{
            last_ts = erlang:now(),
            events = dict:new(),
            last_values = dict:new(),
            message_count = 0
        }}.

handle_call(Call, _From, State) ->
    io:format("UNANDLED handle_call ~p ~p~n", [Call, State]),
    {reply, ok, State}.

handle_info(collection_timer, State) ->
    NextTS = erlang:now(),
    Interval = timer:now_diff(NextTS, State#state.last_ts) / 1000000,
    {NewState1, _, _, EventCount} = dict:fold(fun process_events/3, {State, Interval, NextTS, 0}, State#state.events),

    % Record the number of messages busket is handling per second
    MessageCountAvg = State#state.message_count / Interval,
    busket_store:record(get_unix_timestamp(NextTS), <<"busket.rate">>,
        MessageCountAvg, MessageCountAvg, MessageCountAvg, nil,
        erlang:round(?DEFAULT_INTERVAL/1000), true),
    busket_store:record(get_unix_timestamp(NextTS), <<"busket.unique_events">>,
        EventCount, EventCount, EventCount, nil,
        erlang:round(?DEFAULT_INTERVAL/1000), true),

    NewState2 = NewState1#state{events=dict:new(), message_count=0, last_ts=NextTS},
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
record_events([{?GAUGE_TYPE, Name, Value}|Rest], #state{events=Events, message_count=MessageCount} = State) ->
    {Sum, Min, Max, Count, M, S} = case dict:find({Name, ?GAUGE_TYPE}, Events) of
        {ok, {OldSum, OldMin, OldMax, OldCount, LastM, LastS}} ->
            NewCount = OldCount+1,
            NewM = LastM + (Value - LastM) / NewCount,
            NewS = LastS + (Value - LastM) * (Value - NewM),
            {OldSum+Value, erlang:min(OldMin, Value), erlang:max(OldMax, Value), NewCount, NewM, NewS};
        error ->
            {Value, Value, Value, 1, Value, 0}
    end,
    NewEvents = dict:store({Name, ?GAUGE_TYPE}, {Sum, Min, Max, Count, M, S}, Events),
    NewState = State#state{events=NewEvents, message_count=MessageCount+1},
    record_events(Rest, NewState);
record_events([{?COUNTER_TYPE, Name, Value}|Rest], #state{events=Events, message_count=MessageCount} = State) ->
    NewEvents = dict:store({Name, ?COUNTER_TYPE}, Value, Events),
    NewState = State#state{events=NewEvents, message_count=MessageCount+1},
    record_events(Rest, NewState);
record_events([{?ABSOLUTE_TYPE, Name, Value}|Rest], #state{events=Events, message_count=MessageCount} = State) ->
    NewEvents = dict:update_counter({Name, ?ABSOLUTE_TYPE}, Value, Events),
    NewState = State#state{events=NewEvents, message_count=MessageCount+1},
    record_events(Rest, NewState).

process_events({Name, EventType}, Counters, {State, Interval, TS, Count}) ->
    LastValue = case dict:find({Name, EventType}, State#state.last_values) of
        {ok, LV} ->
            LV;
        error ->
            nil
    end,
    {Avg, Min, Max, Value, Variance} = aggregate_events(EventType, Counters, Interval, LastValue),
    NewState = State#state{last_values=dict:store({Name, EventType}, Value, State#state.last_values)},
    case Avg of
        nil ->
            ok;
        _ ->
            busket_store:record(get_unix_timestamp(TS), Name, Avg, Min, Max,
                Variance, erlang:round(?DEFAULT_INTERVAL/1000), true)
    end,
    {NewState, Interval, TS, Count+1}.

aggregate_events(?ABSOLUTE_TYPE, Sum, Interval, _LastValue) ->
    Avg = Sum/Interval,
    {Avg, Avg, Avg, Sum, nil};
aggregate_events(?COUNTER_TYPE, Value, _Interval, nil) ->
    {nil, nil, nil, Value, nil};
aggregate_events(?COUNTER_TYPE, Value, Interval, LastValue) ->
    Avg = (Value-LastValue)/Interval,
    {Avg, Avg, Avg, Value, nil};
aggregate_events(?GAUGE_TYPE, {Sum, Min, Max, Count, _M, S}, _Interval, _LastValue) ->
    Variance = if
        Count > 1 ->
            S / (Count - 1);
        true ->
            0
    end,
    {Sum/Count, Min, Max, Sum, Variance}.

cleanup() ->
    {ok, Intervals} = application:get_env(busket, intervals),
    cleanup(Intervals).
cleanup([]) ->
    ok;
cleanup([{Resolution, Limit}|Intervals]) ->
    busket_store:cleanup(Resolution, Limit),
    cleanup(Intervals).

rollup() ->
    {ok, [{Resolution, Limit}|Intervals]} = application:get_env(busket, intervals),
    rollup(Intervals, {Resolution, Limit}).
rollup([], _) ->
    ok;
rollup([{Resolution, Limit}|Intervals], {LastResolution, _}) ->
    LastUpdate = busket_store:get_last_update_time(Resolution),
    LastStep = LastUpdate div Resolution,
    Now = get_unix_timestamp(),
    CurrentStep = Now div Resolution,
    if 
        CurrentStep > LastStep ->
            StartTS = CurrentStep * Resolution - Resolution,
            EndTS = StartTS + Limit,
            Events = [proplists:get_value(<<"name">>, E) || E <- busket_store:get_events(StartTS)],
            rollup_aggregate(Events, StartTS, EndTS, Resolution, LastResolution),
            busket_store:set_last_update_time(Resolution, Now);
        true ->
            ok
    end,
    rollup(Intervals, {Resolution, Limit}).

rollup_aggregate([], _, _, _, _) ->
    ok;
rollup_aggregate([Event|Events], StartTS, EndTS, Resolution, LastResolution) ->
    Series = busket_store:get_series(Event, StartTS, EndTS, LastResolution),
    {Sum, Count, Min, Max, Variance} = aggregate(Series),
    if
        Count > 0 ->
            busket_store:record(EndTS, Event, Sum / Count, Min, Max, Variance, Resolution, false);
        true ->
            ok
    end,
    rollup_aggregate(Events, StartTS, EndTS, Resolution, LastResolution).

aggregate(Series) ->
    aggregate(Series, 0, 0, null, null, null, null).
aggregate([], Sum, Count, Min, Max, _M, S) ->
    Variance = if
        Count > 1 ->
            S / (Count - 1);
        true ->
            0
    end,
    {Sum, Count, Min, Max, Variance};
aggregate([Event|Series], Sum, Count, Min, Max, M, S) ->
    Avg = proplists:get_value(<<"avg">>, Event),
    Sum2 = Sum + Avg,
    Min2 = erlang:min(Min, proplists:get_value(<<"min">>, Event)),
    OldMax = proplists:get_value(<<"max">>, Event),
    Max2 = if
        Max == null ->
            OldMax;
        true ->
            erlang:min(Max, proplists:get_value(<<"max">>, Event))
    end,
    Count2 = Count + 1,
    {NewM, NewS} = if
        Count2 == 1 ->
            {Avg, 0};
        true ->
            M2 = M + (Avg - M) / Count2,
            S2 = S + (Avg - M) * (Avg - M2),
            {M2, S2}
    end,
    aggregate(Series, Sum2, Count2, Min2, Max2, NewM, NewS).

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
