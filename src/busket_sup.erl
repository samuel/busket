
-module(busket_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILDA(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    StoreModule = case application:get_env(busket, store_module) of
        undefined ->
            busket_store_debug;
        {ok, SM} ->
            SM
    end,

    Store = ?CHILDA(busket_store, worker, [StoreModule]),
    Busket = ?CHILD(busket, worker),
    Interface = ?CHILD(busket_interface_udp, worker),
    {ok, { {one_for_one, 5, 10}, [Store, Busket, Interface]} }.
