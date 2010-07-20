-module(interface_udp).
-author('Samuel Stauffer <samuel@descolada.com>').

-behaviour(gen_server).

-export([start/0, start_link/0, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_PORT, 5255).

-record(state, {pidparent, socket=not_bound}).

start() ->
    start(?DEFAULT_PORT).
start(Port) ->
    gen_server:start(?MODULE, {self(), Port}, []).

start_link() ->
    start_link(?DEFAULT_PORT).
start_link(Port) ->
    gen_server:start_link(?MODULE, {self(), Port}, []).

stop(Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, stop).

%%%------------------------------------------------------------------------
%%% Callback functions from gen_server
%%%------------------------------------------------------------------------

init({PidParent, Port}) ->
    case gen_udp:open(Port, [binary]) of
        {ok, Socket} ->
            % State#state.pidparent ! {self(), bound},
            State = #state{pidparent=PidParent, socket=Socket}
        % {error, econnrefused} ->
        %     {noreply, State, ?RECONNECT_DELAY}
    end,
    {ok, State}.

handle_call(Call, _From, State) ->
    io:format("UNANDLED handle_call ~p ~p~n", [Call, State]),
    {reply, ok, State, 0}.

handle_info({udp, _Socket, _IP, _InPortNo, Packet}, State) ->
    {ok, NewState} = handle_packet(State, Packet),
    {noreply, NewState};
handle_info(Info,  State) ->
    io:format("UNHANDLED handle_info ~p ~p~n", [Info, State]),
    {noreply, State}.

terminate(Reason, #state{socket=Socket}) ->
    io:format("~p stopping: ~p~n", [?MODULE, Reason]),
    gen_udp:close(Socket),
    ok.

handle_cast(stop, State) ->
    {stop, normal, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%

handle_packet(State, <<>>) ->
    {ok, State};
handle_packet(State, Packet) ->
    <<TypeChar:8, Value:64/big-signed-float, NameLen:8, NameAndRest/binary>> = Packet,
    {Name, Rest} = split_binary(NameAndRest, NameLen),
    busket:record(TypeChar, Name, Value),
    handle_packet(State, Rest).
