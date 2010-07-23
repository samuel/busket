-module(busket_interface_udp).
-author('Samuel Stauffer <samuel@descolada.com>').

-behaviour(gen_server).

-export([start/0, start_link/0, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_PORT, 5252).

-record(state, {pidparent, socket=not_bound, port}).

start() ->
    start(undefined).
start(Port) ->
    gen_server:start(?MODULE, {self(), Port}, []).

start_link() ->
    start_link(undefined).
start_link(Port) ->
    gen_server:start_link(?MODULE, {self(), Port}, []).

stop(Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, stop).

%%%------------------------------------------------------------------------
%%% Callback functions from gen_server
%%%------------------------------------------------------------------------

init({PidParent, Port}) ->
    Port2 = case Port of
        undefined ->
            case application:get_env(busket, udp_port) of
        		undefined ->
        			?DEFAULT_PORT;
        		{ok, P} ->
        		    P
    		end;
    	_ ->
    	    Port
	end,

    io:format("[busket_interface_udp] Listening on port ~p~n", [Port2]),

    case gen_udp:open(Port2, [binary]) of
        {ok, Socket} ->
            % State#state.pidparent ! {self(), bound},
            State = #state{pidparent=PidParent, socket=Socket, port=Port2}
        % {error, econnrefused} ->
        %     {noreply, State, ?RECONNECT_DELAY}
    end,
    {ok, State}.

handle_call(Call, _From, State) ->
    io:format("UNANDLED handle_call ~p ~p~n", [Call, State]),
    {reply, ok, State, 0}.

handle_info({udp, _Socket, _IP, _InPortNo, Packet}, State) ->
    Events = parse_packet(Packet),
    busket:record(Events),
    {noreply, State};
handle_info(Info,  State) ->
    io:format("UNHANDLED handle_info ~p ~p~n", [Info, State]),
    {noreply, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

terminate(Reason, #state{socket=Socket}) ->
    io:format("~p stopping: ~p~n", [?MODULE, Reason]),
    gen_udp:close(Socket),
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%

parse_packet(Packet) ->
    parse_packet(Packet, []).
parse_packet(<<>>, Events) ->
    Events;
parse_packet(<<Type:8, Value:64/big-signed-float, NameLen:8, NameAndRest/binary>> = _Packet, Events) ->
    {Name, Rest} = split_binary(NameAndRest, NameLen),
    % Maintain order of events
    Events2 = parse_packet(Rest, Events),
    [{Type, Name, Value}|Events2];
parse_packet(_, Events) ->
    % TODO: Log the invalid packet
    Events.
