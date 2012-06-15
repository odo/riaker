-module(riaker_sup).
-behaviour(supervisor).

%% API
-export([start_link/1]).
%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Riak_config) ->
	supervisor:start_link({local, ?SERVER}, ?MODULE, Riak_config).

init(Riak_config) ->
	Riak_client_server_sup = {riak_client_server_sup, {riak_client_server_sup, start_link, Riak_config},
						permanent, 2000, supervisor, [riak_client_server_sup]},	
	Ballermann = {ballermann_sup, {ballermann_sup, start_link, [riak_client_server_sup, riak_client_server_pool]},
					permanent, 1000, supervisor, [ballermann_sup, ballermann]},
	Children = [Riak_client_server_sup, Ballermann],
	RestartStrategy = {one_for_one, 10, 10},
	{ok, {RestartStrategy, Children}}.