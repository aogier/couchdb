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
    remove_overlapping_shards/2,
    remove_overlapping_shards/3,
    get_shard_replacements/2
]).

-include_lib("fabric/include/fabric.hrl").
-include_lib("mem3/include/mem3.hrl").



%% @doc looks for a fully covered keyrange in the list of counters
-spec is_progress_possible([{#shard{}, term()}]) -> boolean().
is_progress_possible(Counters) ->
    mem3_util:get_ring(get_worker_ranges(Counters)) =/= [].


-spec remove_overlapping_shards(#shard{}, [{#shard{}, any()}]) ->
    [{#shard{}, any()}].
remove_overlapping_shards(#shard{} = Shard, Counters) ->
    remove_overlapping_shards(Shard, Counters, fun stop_worker/1).


-spec remove_overlapping_shards(#shard{}, [{#shard{}, any()}], fun()) ->
    [{#shard{}, any()}].
remove_overlapping_shards(#shard{} = Shard, Counters, RemoveCb) ->
    Counters1 = filter_exact_copies(Shard, Counters, RemoveCb),
    filter_possible_overlaps(Shard, Counters1, RemoveCb).


filter_possible_overlaps(Shard, Counters, RemoveCb) ->
    Ranges0 = get_worker_ranges(Counters),
    #shard{range = [BShard, EShard]} = Shard,
    Ranges = Ranges0 ++ [{BShard, EShard}],
    {Bs, Es} = lists:unzip(Ranges),
    {MinB, MaxE} = {lists:min(Bs), lists:max(Es)},
    % Use a custom sort function which prioritizes the given shard
    % range when the start endpoints match.
    SortFun  = fun
        ({B, E}, {B, _}) when {B, E} =:= {BShard, EShard} ->
            % If start matches with the shard's start, shard always wins
            true;
        ({B, _}, {B, E}) when {B, E} =:= {BShard, EShard} ->
            % If start matches with te shard's start, shard always wins
            false;
        ({B, E1}, {B, E2}) ->
            % If start matches, pick the longest range first
            E2 >= E1;
        ({B1, _}, {B2, _}) ->
            % Then, by default, sort by start point
            B1 =< B2
    end,
    Ring = mem3_util:get_ring(Ranges, SortFun, MinB, MaxE),
    fabric_dict:filter(fun
        (S, _) when S =:= Shard ->
            % Keep the original shard
            true;
        (#shard{range = [B, E]} = S, _) ->
            case lists:member({B, E}, Ring) of
                true ->
                    true; % Keep it
                false ->
                    % Duplicate range, delete after calling callback function
                    case is_function(RemoveCb) of
                        true -> RemoveCb(S);
                        false -> ok
                    end,
                    false
            end
    end, Counters).


filter_exact_copies(#shard{range = Range0} = Shard0, Shards, Cb) ->
    fabric_dict:filter(fun
        (Shard, _) when Shard =:= Shard0 ->
            true; % Don't remove ourselves
        (#shard{range = Range} = Shard, _) when Range =:= Range0 ->
            case is_function(Cb) of
                true ->  Cb(Shard);
                false -> ok
            end,
            false;
        (_, _) ->
            true
    end, Shards).


stop_worker(#shard{ref = Ref, node = Node}) ->
    rexi:kill(Node, Ref).


get_shard_replacements(DbName, UsedShards0) ->
    % We only want to generate a replacements list from shards
    % that aren't already used.
    AllLiveShards = mem3:live_shards(DbName, [node() | nodes()]),
    UsedShards = [S#shard{ref=undefined} || S <- UsedShards0],
    get_shard_replacements_int(AllLiveShards -- UsedShards, UsedShards).

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


-spec get_worker_ranges([{#shard{}, any()}]) -> [{integer(), integer()}].
get_worker_ranges(Counters) ->
    Ranges = fabric_dict:fold(fun(#shard{range=[X, Y]}, _, Acc) ->
        [{X, Y} | Acc]
    end, [], Counters),
    lists:usort(Ranges).

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


remove_overlapping_shards_test() ->
    Cb = undefined,

    Shards = mk_cnts([[0, 10], [11, 20], [21, ?RING_END]], 3),

    % Simple (exact) overlap
    Shard1 = mk_shard("node-3", [11, 20]),
    Shards1 = fabric_dict:store(Shard1, nil, Shards),
    R1 = remove_overlapping_shards(Shard1, Shards1, Cb),
    ?assertEqual([{0, 10}, {11, 20}, {21, ?RING_END}], get_worker_ranges(R1)),
    ?assert(fabric_dict:is_key(Shard1, R1)),

    % Split overlap (shard overlap multiple workers)
    Shard2 = mk_shard("node-3", [0, 20]),
    Shards2 = fabric_dict:store(Shard2, nil, Shards),
    R2 = remove_overlapping_shards(Shard2, Shards2, Cb),
    ?assertEqual([{0, 20}, {21, ?RING_END}], get_worker_ranges(R2)),
    ?assert(fabric_dict:is_key(Shard2, R2)).


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


mk_cnts(Ranges) ->
    Shards = lists:map(fun mk_shard/1, Ranges),
    orddict:from_list([{Shard,nil} || Shard <- Shards]).

mk_cnts(Ranges, NoNodes) ->
    orddict:from_list([{Shard,nil}
                       || Shard <-
                              lists:flatten(lists:map(
                                 fun(Range) ->
                                         mk_shards(NoNodes,Range,[])
                                 end, Ranges))]
                     ).

mk_shards(0,_Range,Shards) ->
    Shards;
mk_shards(NoNodes,Range,Shards) ->
    Name ="node-" ++ integer_to_list(NoNodes),
    mk_shards(NoNodes-1,Range, [mk_shard(Name, Range) | Shards]).


mk_shard([B, E]) when is_integer(B), is_integer(E) ->
    #shard{range = [B, E]}.


mk_shard(Name, Range) ->
    Node = list_to_atom(Name),
    BName = list_to_binary(Name),
    #shard{name=BName, node=Node, range=Range}.
