-module(sobhuza).
-behavior(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    nodes
}).

start_link(Nodes) ->
    Nodes = [N || N <- Nodes, is_atom(N)],
    gen_server({local, ?MODULE}, ?MODULE, Nodes, []).


init(Nodes) ->
    {ok, #state{nodes = Nodes}}.


handle_call(_Msg, _From, State) ->
    {noreply, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Msg, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
