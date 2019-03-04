% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric_ring).


-export([
    is_progress_possible/1,
    get_shard_replacements/2,
    worker_exited/3,
    node_down/3,
    handle_error/3,
    handle_response/4

]).


-include_lib("fabric/include/fabric.hrl").
-include_lib("mem3/include/mem3.hrl").


-type fabric_dict() :: [{#shard{}, any()}].


%% @doc looks for a fully covered keyrange in the list of counters
-spec is_progress_possible(fabric_dict()) -> boolean().
is_progress_possible(Counters) ->
    mem3_util:get_ring(get_worker_ranges(Counters)) =/= [].


-spec get_shard_replacements(binary(), [#shard{}]) -> [#shard{}].
get_shard_replacements(DbName, UsedShards0) ->
    % We only want to generate a replacements list from shards
    % that aren't already used.
    AllLiveShards = mem3:live_shards(DbName, [node() | nodes()]),
    UsedShards = [S#shard{ref=undefined} || S <- UsedShards0],
    get_shard_replacements_int(AllLiveShards -- UsedShards, UsedShards).


-spec worker_exited(#shard{}, fabric_dict(), [{any(), #shard{}, any()}]) ->
    {ok, fabric_dict()} | error.
worker_exited(Shard, Workers, Responses) ->
    Workers1 = fabric_dict:erase(Shard, Workers),
    case is_progress_possible(Workers1, Responses) of
        true -> {ok, Workers1};
        false -> error
    end.


-spec node_down(node(), fabric_dict(), fabric_dict()) ->
    {ok, fabric_dict()} | error.
node_down(Node, Workers, Responses) ->
    Workers1 = fabric_dict:filter(fun(#shard{node = N}, _) ->
        N =/= Node
    end, Workers),
    case is_progress_possible(Workers1, Responses) of
        true -> {ok, Workers1};
        false -> error
    end.


-spec handle_error(#shard{}, fabric_dict(), fabric_dict()) ->
    {ok, fabric_dict()} | error.
handle_error(Shard, Workers, Responses) ->
    Workers1 = fabric_dict:erase(Shard, Workers),
    case is_progress_possible(Workers1, Responses) of
        true -> {ok, Workers1};
        false -> error
    end.


-spec handle_response(#shard{}, any(), fabric_dict(), fabric_dict()) ->
    {ok, {fabric_dict(), fabric_dict()}} | {stop, fabric_dict()}.
handle_response(Shard, Response, Workers, Responses) ->
    handle_response(Shard, Response, Workers, Responses, fun stop_workers/1).


-spec handle_response(#shard{}, any(), fabric_dict(), fabric_dict(), fun()) ->
    {ok, {fabric_dict(), fabric_dict()}} | {stop, list()}.
handle_response(Shard, Response, Workers, Responses, CleanupCb) ->
    Workers1 = fabric_dict:erase(Shard, Workers),
    #shard{range = [B, E]} = Shard,
    Responses1 = [{{B, E}, Shard, Response} | Responses],
    ResponseRanges = lists:map(fun({R, _, _}) -> R end, Responses1),
    {MinB, MaxE} = get_range_bounds(Workers, ResponseRanges),
    case mem3_util:get_ring(ResponseRanges, MinB, MaxE) of
        [] ->
            {ok, {Workers1, Responses1}};
        Ring ->
            % Return one response per range in the ring. The
            % response list is reversed before sorting so that the
            % first shard copy to reply is first. We use keysort
            % because it is documented as being stable so that
            % we keep the relative order of duplicate shards
            SortedResponses = lists:keysort(1, lists:reverse(Responses1)),
            UsedResponses = get_responses(Ring, SortedResponses),
            % Kill all the remaining workers as well as the redunant responses
            stop_unused_workers(Workers1, Responses1, UsedResponses, CleanupCb),
            {stop, UsedResponses}
    end.


% This version combines workers that are still waiting and the ones that have
% responded already.
-spec is_progress_possible(fabric_dict(), [{any(), #shard{}, any()}]) ->
    boolean().
is_progress_possible(Counters, Responses) ->
    ResponseRanges = lists:map(fun({{B, E}, _, _}) -> {B, E} end, Responses),
    {MinB, MaxE} = get_range_bounds(Counters, ResponseRanges),
    Ranges = get_worker_ranges(Counters) ++ ResponseRanges,
    mem3_util:get_ring(Ranges, MinB, MaxE) =/= [].


get_shard_replacements_int(UnusedShards, UsedShards) ->
    % If we have more than one copy of a range then we don't
    % want to try and add a replacement to any copy.
    RangeCounts = lists:foldl(fun(#shard{range=R}, Acc) ->
        dict:update_counter(R, 1, Acc)
    end, dict:new(), UsedShards),

    % For each seq shard range with a count of 1, find any
    % possible replacements from the unused shards. The
    % replacement list is keyed by range.
    lists:foldl(fun(#shard{range = [B, E] = Range}, Acc) ->
        case dict:find(Range, RangeCounts) of
            {ok, 1} ->
                Repls = mem3_util:non_overlapping_shards(UnusedShards, B, E),
                % Only keep non-empty lists of replacements
                if Repls == [] -> Acc; true ->
                    [{Range, Repls} | Acc]
                end;
            _ ->
                Acc
        end
    end, [], UsedShards).


-spec get_worker_ranges(fabric_dict()) -> [{integer(), integer()}].
get_worker_ranges(Workers) ->
    Ranges = fabric_dict:fold(fun(#shard{range=[X, Y]}, _, Acc) ->
        [{X, Y} | Acc]
    end, [], Workers),
    lists:usort(Ranges).


get_range_bounds(Workers, ResponseRanges) ->
    Ranges = get_worker_ranges(Workers) ++ ResponseRanges,
    {Bs, Es} = lists:unzip(Ranges),
    {lists:min(Bs), lists:max(Es)}.


get_responses([], _) ->
    [];

get_responses([Range | Ranges], [{Range, Shard, Value} | Resps]) ->
    [{Shard, Value} | get_responses(Ranges, Resps)];

get_responses(Ranges, [_DupeRangeResp | Resps]) ->
    get_responses(Ranges, Resps).


stop_unused_workers(_, _, _, undefined) ->
    ok;

stop_unused_workers(Workers, AllResponses, UsedResponses, CleanupCb) ->
    WorkerShards = [S || {S, _V} <- Workers],
    Used  = [S || {S, _V} <- UsedResponses],
    Unused = [S || {_R, S, _V} <- AllResponses, not lists:member(S, Used)],
    CleanupCb(WorkerShards ++ Unused).


stop_workers(Shards) when is_list(Shards) ->
    rexi:kill_all([{Node, Ref} || #shard{node = Node, ref = Ref} <- Shards]).


% Unit tests

is_progress_possible_test() ->
    T1 = [[0, ?RING_END]],
    ?assertEqual(is_progress_possible(mk_cnts(T1)), true),
    T2 = [[0, 10], [11, 20], [21, ?RING_END]],
    ?assertEqual(is_progress_possible(mk_cnts(T2)), true),
    % gap
    T3 = [[0, 10], [12, ?RING_END]],
    ?assertEqual(is_progress_possible(mk_cnts(T3)), false),
    % outside range
    T4 = [[1, 10], [11, 20], [21, ?RING_END]],
    ?assertEqual(is_progress_possible(mk_cnts(T4)), false),
    % outside range
    T5 = [[0, 10], [11, 20], [21, ?RING_END + 1]],
    ?assertEqual(is_progress_possible(mk_cnts(T5)), false),
    % possible progress but with backtracking
    T6 = [[0, 10], [11, 20], [0, 5], [6, 21], [21, ?RING_END]],
    ?assertEqual(is_progress_possible(mk_cnts(T6)), true),
    % not possible, overlap is not exact
    T7 = [[0, 10], [13, 20], [21, ?RING_END], [9, 12]],
    ?assertEqual(is_progress_possible(mk_cnts(T7)), false).


get_shard_replacements_test() ->
    Unused = [mk_shard(N, [B, E]) || {N, B, E} <- [
        {"n1", 11, 20}, {"n1", 21, ?RING_END},
        {"n2", 0, 4}, {"n2", 5, 10}, {"n2", 11, 20},
        {"n3", 0, 21, ?RING_END}
    ]],
    Used = [mk_shard(N, [B, E]) || {N, B, E} <- [
        {"n2", 21, ?RING_END},
        {"n3", 0, 10}, {"n3", 11, 20}
    ]],
    Res = lists:sort(get_shard_replacements_int(Unused, Used)),
    % Notice that [0, 10] range can be replaces by spawning the
    % [0, 4] and [5, 10] workers on n1
    Expect = [
        {[0, 10], [mk_shard("n2", [0, 4]), mk_shard("n2", [5, 10])]},
        {[11, 20], [mk_shard("n1", [11, 20]), mk_shard("n2", [11, 20])]},
        {[21, ?RING_END], [mk_shard("n1", [21, ?RING_END])]}
    ],
    ?assertEqual(Expect, Res).


handle_response_basic_test() ->
    Shard1 = mk_shard("n1", [0, 1]),
    Shard2 = mk_shard("n1", [2, ?RING_END]),

    Workers1 = fabric_dict:init([Shard1, Shard2], nil),

    Result1 = handle_response(Shard1, 42, Workers1, [], undefined),
    ?assertMatch({ok, {_, _}}, Result1),
    {ok, {Workers2, Responses1}} = Result1,
    ?assertEqual(fabric_dict:erase(Shard1, Workers1), Workers2),
    ?assertEqual([{{0, 1}, Shard1, 42}], Responses1),

    Result2 = handle_response(Shard2, 43, Workers2, Responses1, undefined),
    ?assertEqual({stop, [{Shard1, 42}, {Shard2, 43}]}, Result2).


handle_response_incomplete_ring_test() ->
    Shard1 = mk_shard("n1", [0, 1]),
    Shard2 = mk_shard("n1", [2, 10]),

    Workers1 = fabric_dict:init([Shard1, Shard2], nil),

    Result1 = handle_response(Shard1, 42, Workers1, [], undefined),
    ?assertMatch({ok, {_, _}}, Result1),
    {ok, {Workers2, Responses1}} = Result1,
    ?assertEqual(fabric_dict:erase(Shard1, Workers1), Workers2),
    ?assertEqual([{{0, 1}, Shard1, 42}], Responses1),

    Result2 = handle_response(Shard2, 43, Workers2, Responses1, undefined),
    ?assertEqual({stop, [{Shard1, 42}, {Shard2, 43}]}, Result2).


handle_response_test_multiple_copies_test() ->
    Shard1 = mk_shard("n1", [0, 1]),
    Shard2 = mk_shard("n2", [0, 1]),
    Shard3 = mk_shard("n1", [2, ?RING_END]),

    Workers1 = fabric_dict:init([Shard1, Shard2, Shard3], nil),

    Result1 = handle_response(Shard1, 42, Workers1, [], undefined),
    ?assertMatch({ok, {_, _}}, Result1),
    {ok, {Workers2, Responses1}} = Result1,

    Result2 = handle_response(Shard2, 43, Workers2, Responses1, undefined),
    ?assertMatch({ok, {_, _}}, Result2),
    {ok, {Workers3, Responses2}} = Result2,

    Result3 = handle_response(Shard3, 44, Workers3, Responses2, undefined),
    % Use the value (42) to distinguish between [0, 1] copies. In reality
    % they should have the same value but here we need to assert that copy
    % that responded first is included in the ring.
    ?assertEqual({stop, [{Shard1, 42}, {Shard3, 44}]}, Result3).


handle_response_test_backtracking_test() ->
    Shard1 = mk_shard("n1", [0, 5]),
    Shard2 = mk_shard("n1", [10, ?RING_END]),
    Shard3 = mk_shard("n2", [2, ?RING_END]),
    Shard4 = mk_shard("n3", [0, 1]),

    Workers1 = fabric_dict:init([Shard1, Shard2, Shard3, Shard3], nil),

    Result1 = handle_response(Shard1, 42, Workers1, [], undefined),
    ?assertMatch({ok, {_, _}}, Result1),
    {ok, {Workers2, Responses1}} = Result1,

    Result2 = handle_response(Shard2, 43, Workers2, Responses1, undefined),
    ?assertMatch({ok, {_, _}}, Result2),
    {ok, {Workers3, Responses2}} = Result2,

    Result3 = handle_response(Shard3, 44, Workers3, Responses2, undefined),
    ?assertMatch({ok, {_, _}}, Result3),
    {ok, {Workers4, Responses3}} = Result3,

    Result4 = handle_response(Shard4, 45, Workers4, Responses3, undefined),
    ?assertEqual({stop, [{Shard4, 45}, {Shard3, 44}]}, Result4).


handle_error_test() ->
    Shard1 = mk_shard("n1", [0, 5]),
    Shard2 = mk_shard("n1", [10, ?RING_END]),
    Shard3 = mk_shard("n2", [2, ?RING_END]),
    Shard4 = mk_shard("n3", [0, 1]),

    Workers1 = fabric_dict:init([Shard1, Shard2, Shard3, Shard4], nil),

    Result1 = handle_response(Shard1, 42, Workers1, [], undefined),
    ?assertMatch({ok, {_, _}}, Result1),
    {ok, {Workers2, Responses1}} = Result1,

    Result2 = handle_error(Shard2, Workers2, Responses1),
    ?assertMatch({ok, _}, Result2),
    {ok, Workers3} = Result2,
    ?assertEqual(fabric_dict:erase(Shard2, Workers2), Workers3),

    Result3 = handle_response(Shard3, 44, Workers3, Responses1, undefined),
    ?assertMatch({ok, {_, _}}, Result3),
    {ok, {Workers4, Responses3}} = Result3,

    ?assertEqual(error, handle_error(Shard4, Workers4, Responses3)).


node_down_test() ->
    Shard1 = mk_shard("n1", [0, 5]),
    Shard2 = mk_shard("n1", [10, ?RING_END]),
    Shard3 = mk_shard("n2", [2, ?RING_END]),
    Shard4 = mk_shard("n3", [0, 1]),

    Workers1 = fabric_dict:init([Shard1, Shard2, Shard3, Shard4], nil),

    Result1 = handle_response(Shard1, 42, Workers1, [], undefined),
    ?assertMatch({ok, {_, _}}, Result1),
    {ok, {Workers2, Responses1}} = Result1,

    Result2 = handle_response(Shard2, 43, Workers2, Responses1, undefined),
    ?assertMatch({ok, {_, _}}, Result2),
    {ok, {Workers3, Responses2}} = Result2,

    Result3 = node_down(n1, Workers3, Responses2),
    ?assertMatch({ok, _}, Result3),
    {ok, Workers4} = Result3,
    ?assertEqual([{Shard3, nil}, {Shard4, nil}], Workers4),

    Result4 = handle_response(Shard3, 44, Workers4, Responses2, undefined),
    ?assertMatch({ok, {_, _}}, Result4),
    {ok, {Workers5, Responses3}} = Result4,

    % Note: Shard3 was already processed, it's ok if n2 went down after
    ?assertEqual({ok, [{Shard4, nil}]}, node_down(n2, Workers5, Responses3)),

    ?assertEqual(error, node_down(n3, Workers5, Responses3)).


mk_cnts(Ranges) ->
    Shards = lists:map(fun mk_shard/1, Ranges),
    orddict:from_list([{Shard,nil} || Shard <- Shards]).


mk_shard([B, E]) when is_integer(B), is_integer(E) ->
    #shard{range = [B, E]}.


mk_shard(Name, Range) ->
    Node = list_to_atom(Name),
    BName = list_to_binary(Name),
    #shard{name=BName, node=Node, range=Range}.
