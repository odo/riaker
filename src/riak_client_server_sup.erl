-module(riak_client_server_sup).
-behaviour(supervisor).

%% API
-export ([
	start_link/0,
	start_link/1
]).

%% Callbacks
-export ([init/1]).
-define (SERVER, ?MODULE).

start_link() ->
	start_link({}).

start_link({}) ->
	start_link({10, "127.0.0.1", 8087, []});

start_link({Child_count}) ->
	start_link({Child_count, "127.0.0.1", 8087, []});

start_link({Child_count, IP}) ->
	start_link({Child_count, IP, 8087, []});

start_link({Child_count, IP, Port}) ->
	start_link({Child_count, IP, Port, []});
	
start_link({Child_count, IP, Port, Options}) ->
	error_logger:info_msg("Starting riakle with options: ~p\n", [{Child_count, IP, Port, Options}]),
	case supervisor:start_link({local, ?SERVER}, ?MODULE, [IP, Port, Options]) of
		{ok, Pid} ->
			[start_child() || _ <- lists:seq(1, Child_count)],
			{ok, Pid};
		_ ->
			throw({error, cannot_connect_to_riak_server, IP, Port, Options})
		end.
		
start_child() ->
	supervisor:start_child(?SERVER, []).
	
init([IP, Port, Options]) ->
	Server = {riak_client_server, {riakc_pb_socket, start_link, [IP, Port, Options]},
						permanent, 1000, worker, [riakc_pb_socket, riakc_pb, riakle]},
	Children = [Server],
	RestartStrategy = {simple_one_for_one, 10, 1},
	{ok, {RestartStrategy, Children}}.