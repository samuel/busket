-module(busket).
-author('Samuel Stauffer <samuel@descolada.com>').

-behaviour(gen_server).

-export([start_link/0, start/0]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_info/2, handle_cast/2]).

-define(ABSOLUTE_TYPE, 97).
-define(COUNTER_TYPE, 99).
-define(GAUGE_TYPE, 103).
-define(DEFAULT_INTERVAL, 1000). %60000). % ms
-define(INTERVALS, [
    {5*60, 576},     % Every 5 minutes for 48 hours
    {30*60, 432},    % Every 30 minutes for 9 days
    {60*60, 1080},   % Every 1 hours for 45 days
    {24*60*60, 450}  % Every day for 450 days
]).

-record(state, {endpoints, last_ts, events, last_values, collector}).

start_link() ->
    gen_server:start_link(?MODULE, {self()}, []).

start() ->
    gen_server:start(?MODULE, {self()}, []).

%% gen_server callbacks

init({_PidMaster}) ->
    timer:start(),
    UDPEndPoint = udp_endpoint:start(),
    timer:send_after(time_to_next_interval(?DEFAULT_INTERVAL), collection_timer),
    {ok, Collector} = collector_mongo:start(),
    {ok, #state{
            endpoints = [UDPEndPoint],
            last_ts = erlang:now(),
            events = dict:new(),
            last_values = dict:new(),
            collector = Collector
        }}.

handle_call(Call, _From, State) ->
    io:format("UNANDLED handle_call ~p ~p~n", [Call, State]),
    {reply, ok, State}.

handle_info({_EndPoint, event, {EventType, Name, Value}}, #state{events=Events} = State) ->
    NewState = State#state{events=dict:append({Name, EventType}, Value, Events)},
    {noreply, NewState};
handle_info(collection_timer, State) ->
    NextTS = erlang:now(),
    Interval = timer:now_diff(NextTS, State#state.last_ts) / 1000000,
    io:format("~p~n", [Interval]),
    {NewState1, _, _} = dict:fold(fun process_events/3, {State, Interval, NextTS}, State#state.events),
    NewState2 = NewState1#state{events=dict:new(), last_ts=NextTS},
    % NewState3 = aggregate(get_unix_timestamp(NextTS), NewState2),
    timer:send_after(time_to_next_interval(?DEFAULT_INTERVAL), collection_timer),
    {noreply, NewState2};
% handle_info(aggregate_timer, State) ->
%     {noreply, State};
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

%%%

% aggregate(TS, State) ->
%     aggregate(?INTERVALS, TS, State),
%     State.
% aggregate([{Resolution,Limit}|Intervals], TS, State) ->
%     void.

process_events({Name, EventType}, Events, {State, Interval, TS}) ->
    LastValue = case dict:find({Name, EventType}, State#state.last_values) of
        {ok, LV} ->
            LV;
        error ->
            nil
    end,
    {Avg, Min, Max, Value} = aggregate_events(EventType, Events, Interval, LastValue),
    NewState = State#state{last_values=dict:store({Name, EventType}, Value, State#state.last_values)},
    % io:format("~p ~p ~p ~p ~p ~p~n", [get_unix_timestamp(TS), Name, Avg, Min, Max, Value]),
    NewState#state.collector ! {record, get_unix_timestamp(TS), Name, Avg, Min, Max, erlang:round(?DEFAULT_INTERVAL/1000)},
    {NewState, Interval, TS}.

aggregate_events(?ABSOLUTE_TYPE, Events, Interval, _LastValue) ->
    Sum = lists:sum(Events),
    Avg = Sum/Interval,
    {Avg, Avg, Avg, Sum};
aggregate_events(?COUNTER_TYPE, Events, _Interval, nil) ->
    Value = lists:last(Events),
    {nil, nil, nil, Value};
aggregate_events(?COUNTER_TYPE, Events, Interval, LastValue) ->
    Value = lists:last(Events),
    Avg = (Value-LastValue)/Interval,
    {Avg, Avg, Avg, Value};
aggregate_events(?GAUGE_TYPE, [Value|Events], _Interval, _LastValue) ->
    aggregate_events_gauge(Events, Value, Value, Value, 1).
aggregate_events_gauge([Value|Events], Sum, Min, Max, Count) ->
    aggregate_events_gauge(Events, Sum+Value, erlang:min(Value, Min), erlang:max(Value, Max), Count+1);
aggregate_events_gauge([], Sum, Min, Max, Count) ->
    {Sum/Count, Min, Max, Sum}.

time_to_next_interval(Interval) ->
    {Megaseconds, Seconds, Microseconds} = erlang:now(),
    MS = Interval - (erlang:round(timer:seconds(Megaseconds*1000000+Seconds)+Microseconds/1000) rem Interval),
    MS2 = case MS < 1000 of
        true -> MS + Interval;
        false -> MS
    end,
    MS2.

get_unix_timestamp(TS) ->
    calendar:datetime_to_gregorian_seconds( calendar:now_to_universal_time(TS) ) -
        calendar:datetime_to_gregorian_seconds( {{1970,1,1},{0,0,0}} ).
