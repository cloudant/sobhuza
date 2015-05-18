-module(sobhuza).
-behavior(gen_server).

-export([start_link/0, start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    delta,
    last_alert,
    last_ping,
    last_restart,
    leader,
    nodes,
    ok_count,
    pongs,
    round,
    tick,
    timer
}).

start_link() ->
    sobhuza:start_link(500, mem3:nodes()).


start_link(Delta, Nodes) when is_integer(Delta), Delta > 10, is_list(Nodes) ->
    case lists:member(node(), Nodes) of
	true ->
	    gen_server:start_link({local, ?MODULE}, ?MODULE, {Delta, Nodes}, []);
	false ->
	    {error, not_member}
    end.


init({Delta, Nodes}) ->
    State = #state{
        delta = Delta,
	last_alert = 0,
        last_restart = 0,
	nodes = lists:sort(Nodes),
        ok_count = 0,
	pongs = [],
	tick = 0,
	timer = timer:send_interval(Delta, tick)
    },
    {ok, start_round(0, State)}.


handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast({ts, Timestamp, Nodes, Msg}, State) ->
    case timer:now_diff(os:timestamp(), Timestamp) > (State#state.delta * 1000) orelse
        State#state.nodes /= Nodes of
	true ->
	    % message has expired or cohort is distinct
	    {noreply, State};
	false ->
	    handle_cast(Msg, State)
    end;

handle_cast({ok, Round, _From}, State0) when Round == State0#state.round ->
    State1 = restart_timer(State0#state{ok_count = State0#state.ok_count + 1}),
    case (State1#state.leader == undefined andalso
	  State1#state.ok_count >= 2 andalso
          (State1#state.tick - State1#state.last_alert) >= 6) of
	true ->
	    State2 = State1#state{round = Round},
	    Leader = candidate(State2),
	    twig:log(notice, "~p is the leader~n", [Leader]),
	    {noreply, State2#state{leader = Leader}};
	false ->
	    {noreply, State1}
    end;

handle_cast({ok, Round, _From}, State) when Round > State#state.round ->
    {noreply, start_round(Round, State)};

handle_cast({start, Round, _From}, State) when Round > State#state.round ->
    {noreply, start_round(Round, State)};

handle_cast({ok, Round, From}, State) when Round < State#state.round ->
    send({ok, State#state.round, node()}, From, State),
    {noreply, State};

handle_cast({start, Round, From}, State) when Round < State#state.round ->
    send({ok, State#state.round, node()}, From, State),
    {noreply, State};

handle_cast({alert, Round}, State) when Round > State#state.round ->
    twig:log(notice, "Leader demoted by alert from round ~B~n", [Round]),
    {noreply, State#state{last_alert = State#state.tick, leader = undefined}};

handle_cast({ping, Round, From}, State) ->
    twig:log(notice, "Received ping from ~p in round ~B~n", [From, Round]),
    send({pong, Round, node()}, From, State),
    {noreply, State};

handle_cast({pong, Round, From}, State0) when Round == State0#state.round ->
    twig:log(notice, "Received pong from ~p in current round ~B~n", [From, Round]),
    State1 = State0#state{pongs = [From | State0#state.pongs]},
    {noreply, State1};

handle_cast({pong, Round, From}, State) ->
    twig:log(notice, "Received pong from ~p in round ~B~n", [From, Round]),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(tick, State) ->
    NewNodes = lists:sort(mem3:nodes()),
    NodesChanged = State#state.nodes /= NewNodes,
    Pinging = State#state.last_ping /= undefined,
    PingComplete = Pinging andalso (State#state.tick - State#state.last_ping) == 2,
    Timeout = (State#state.tick - State#state.last_restart) > 2,
    case {NodesChanged, Pinging, PingComplete, Timeout} of
        {true, _, _, _} ->
            {ok, State1} = init({State#state.delta, NewNodes}),
            {noreply, State1};
	{false, true, false, _} ->
	    {noreply, tick(State)};
	{false, true, true, _} ->
	    Round = next_round(State),
	    twig:log(notice, "Jumping to responsive round ~B~n", [Round]),
	    {noreply, tick(start_round(Round, State#state{last_ping = undefined}))};
	{false, false, false, true} ->
	    send_all({alert, State#state.round + 1}, State),
	    send_all({ping, State#state.round, node()}, State),
	    {noreply, tick(State#state{last_ping = State#state.tick})};
	{false, false, false, false} ->
	    case candidate(State) of
		Node when Node == node() ->
		    send_all({ok, State#state.round, node()}, State);
		_ ->
		    ok
	    end,
	    {noreply, tick(State)}
    end.


terminate(_Reason, State) ->
    timer:cancel(State#state.timer),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% private functions

candidate(#state{round = Round, nodes = Nodes}) ->
    candidate(Round, Nodes).

candidate(Round, Nodes) ->
    lists:nth(Round rem length(Nodes) + 1, Nodes).


next_round(#state{round = Round, nodes = Nodes, pongs = Pongs}) ->
    L0 = lists:seq(Round + 1, Round + 1 + length(Nodes)),
    L1 = [{S, candidate(S, Nodes)} || S <- L0],
    L2 = [S || {S, Candidate} <- L1, lists:member(Candidate, Pongs)],
    hd(L2).


tick(State) ->
    State#state{tick = State#state.tick + 1}.


restart_timer(State) ->
    State#state{last_restart = State#state.tick}.


send(Msg, Node, State) ->
    gen_server:cast({?MODULE, Node}, {ts, os:timestamp(), State#state.nodes, Msg}).


send_all(Msg, State) ->
    [send(Msg, Node, State) || Node <- State#state.nodes].


start_round(Round, State0) ->
    twig:log(notice, "Starting round ~B~n", [Round]),
    State = restart_timer(State0#state{
        last_ping = undefined,
        leader = undefined,
        ok_count = 0,
	round = Round,
        pongs = []
    }),
    send_all({alert, Round}, State),
    case candidate(State) of
	Node when Node /= node() ->
	    send_all({start, Round, node()}, State);
	_ ->
	    ok
    end,
    State.
