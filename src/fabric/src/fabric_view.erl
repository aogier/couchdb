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

-module(fabric_view).

-export([
    maybe_send_row/1,
    transform_row/1,
    keydict/1,
    extract_view/4,
    get_shards/2,
    check_down_shards/2,
    handle_worker_exit/3,
    maybe_update_others/5,
    fix_skip_and_limit/1
]).


-include_lib("fabric/include/fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").

%% @doc Check if a downed node affects any of our workers
-spec check_down_shards(#collector{}, node()) ->
    {ok, #collector{}} | {error, any()}.
check_down_shards(Collector, BadNode) ->
    #collector{callback=Callback, counters=Counters, user_acc=Acc} = Collector,
    Filter = fun(#shard{node = Node}, _) -> Node == BadNode end,
    BadCounters = fabric_dict:filter(Filter, Counters),
    case fabric_dict:size(BadCounters) > 0 of
        true ->
            Reason = {nodedown, <<"progress not possible">>},
            Callback({error, Reason}, Acc),
            {error, Reason};
        false ->
            {ok, Collector}
    end.

%% @doc Handle a worker that dies during a stream
-spec handle_worker_exit(#collector{}, #shard{}, any()) -> {error, any()}.
handle_worker_exit(Collector, _Worker, Reason) ->
    #collector{callback=Callback, user_acc=Acc} = Collector,
    {ok, Resp} = Callback({error, fabric_util:error_info(Reason)}, Acc),
    {error, Resp}.


maybe_send_row(#collector{limit=0} = State) ->
    #collector{counters=Counters, user_acc=AccIn, callback=Callback} = State,
    case fabric_dict:any(0, Counters) of
    true ->
        % we still need to send the total/offset header
        {ok, State};
    false ->
        erase(meta_sent),
        {_, Acc} = Callback(complete, AccIn),
        {stop, State#collector{user_acc=Acc}}
    end;
maybe_send_row(State) ->
    #collector{
        callback = Callback,
        counters = Counters,
        skip = Skip,
        limit = Limit,
        user_acc = AccIn
    } = State,
    case fabric_dict:any(0, Counters) of
    true ->
        {ok, State};
    false ->
        try get_next_row(State) of
        {_, NewState} when Skip > 0 ->
            maybe_send_row(NewState#collector{skip=Skip-1});
        {Row0, NewState} ->
            Row1 = possibly_embed_doc(NewState, Row0),
            Row2 = detach_partition(Row1),
            Row3 = transform_row(Row2),
            case Callback(Row3, AccIn) of
            {stop, Acc} ->
                {stop, NewState#collector{user_acc=Acc, limit=Limit-1}};
            {ok, Acc} ->
                maybe_send_row(NewState#collector{user_acc=Acc, limit=Limit-1})
            end
        catch complete ->
            erase(meta_sent),
            {_, Acc} = Callback(complete, AccIn),
            {stop, State#collector{user_acc=Acc}}
        end
    end.

%% if include_docs=true is used when keys and
%% the values contain "_id" then use the "_id"s
%% to retrieve documents and embed in result
possibly_embed_doc(_State,
              #view_row{id=reduced}=Row) ->
    Row;
possibly_embed_doc(_State,
                   #view_row{value=undefined}=Row) ->
    Row;
possibly_embed_doc(#collector{db_name=DbName, query_args=Args},
              #view_row{key=_Key, id=_Id, value=Value, doc=_Doc}=Row) ->
    #mrargs{include_docs=IncludeDocs} = Args,
    case IncludeDocs andalso is_tuple(Value) of
    true ->
        {Props} = Value,
        Rev0 = couch_util:get_value(<<"_rev">>, Props),
        case couch_util:get_value(<<"_id">>,Props) of
        null -> Row#view_row{doc=null};
        undefined -> Row;
        IncId ->
            % use separate process to call fabric:open_doc
            % to not interfere with current call
            {Pid, Ref} = spawn_monitor(fun() ->
                exit(
                case Rev0 of
                undefined ->
                    case fabric:open_doc(DbName, IncId, []) of
                    {ok, NewDoc} ->
                        Row#view_row{doc=couch_doc:to_json_obj(NewDoc,[])};
                    {not_found, _} ->
                        Row#view_row{doc=null};
                    Else ->
                        Row#view_row{doc={error, Else}}
                    end;
                Rev0 ->
                    Rev = couch_doc:parse_rev(Rev0),
                    case fabric:open_revs(DbName, IncId, [Rev], []) of
                    {ok, [{ok, NewDoc}]} ->
                        Row#view_row{doc=couch_doc:to_json_obj(NewDoc,[])};
                    {ok, [{{not_found, _}, Rev}]} ->
                        Row#view_row{doc=null};
                    Else ->
                        Row#view_row{doc={error, Else}}
                    end
                end) end),
            receive {'DOWN',Ref,process,Pid, Resp} ->
                        Resp
            end
        end;
        _ -> Row
    end.

detach_partition(#view_row{key={p, _Partition, Key}} = Row) ->
    Row#view_row{key = Key};
detach_partition(#view_row{} = Row) ->
    Row.

keydict(undefined) ->
    undefined;
keydict(Keys) ->
    {Dict,_} = lists:foldl(fun(K, {D,I}) -> {dict:store(K,I,D), I+1} end,
        {dict:new(),0}, Keys),
    Dict.

%% internal %%

get_next_row(#collector{rows = []}) ->
    throw(complete);
get_next_row(#collector{reducer = RedSrc} = St) when RedSrc =/= undefined ->
    #collector{
        query_args = #mrargs{direction = Dir},
        keys = Keys,
        rows = RowDict,
        lang = Lang,
        counters = Counters0,
        collation = Collation
    } = St,
    {Key, RestKeys} = find_next_key(Keys, Dir, Collation, RowDict),
    case dict:find(Key, RowDict) of
    {ok, Records} ->
        NewRowDict = dict:erase(Key, RowDict),
        Counters = lists:foldl(fun(#view_row{worker={Worker,From}}, CntrsAcc) ->
            case From of
                {Pid, _} when is_pid(Pid) ->
                    gen_server:reply(From, ok);
                Pid when is_pid(Pid) ->
                    rexi:stream_ack(From)
            end,
            fabric_dict:update_counter(Worker, -1, CntrsAcc)
        end, Counters0, Records),
        Wrapped = [[V] || #view_row{value=V} <- Records],
        {ok, [Reduced]} = couch_query_servers:rereduce(Lang, [RedSrc], Wrapped),
        {ok, Finalized} = couch_query_servers:finalize(RedSrc, Reduced),
        NewSt = St#collector{keys=RestKeys, rows=NewRowDict, counters=Counters},
        {#view_row{key=Key, id=reduced, value=Finalized}, NewSt};
    error ->
        get_next_row(St#collector{keys=RestKeys})
    end;
get_next_row(State) ->
    #collector{rows = [Row|Rest], counters = Counters0} = State,
    {Worker, From} = Row#view_row.worker,
    rexi:stream_ack(From),
    Counters1 = fabric_dict:update_counter(Worker, -1, Counters0),
    {Row, State#collector{rows = Rest, counters=Counters1}}.

%% TODO: rectify nil <-> undefined discrepancies
find_next_key(nil, Dir, Collation, RowDict) ->
    find_next_key(undefined, Dir, Collation, RowDict);
find_next_key(undefined, Dir, Collation, RowDict) ->
    CmpFun = fun(A, B) -> compare(Dir, Collation, A, B) end,
    case lists:sort(CmpFun, dict:fetch_keys(RowDict)) of
    [] ->
        throw(complete);
    [Key|_] ->
        {Key, nil}
    end;
find_next_key([], _, _, _) ->
    throw(complete);
find_next_key([Key|Rest], _, _, _) ->
    {Key, Rest}.

transform_row(#view_row{value={[{reduce_overflow_error, Msg}]}}) ->
    {row, [{key,null}, {id,error}, {value,reduce_overflow_error}, {reason,Msg}]};
transform_row(#view_row{key=Key, id=reduced, value=Value}) ->
    {row, [{key,Key}, {value,Value}]};
transform_row(#view_row{key=Key, id=undefined}) ->
    {row, [{key,Key}, {id,error}, {value,not_found}]};
transform_row(#view_row{key=Key, id=Id, value=Value, doc=undefined}) ->
    {row, [{id,Id}, {key,Key}, {value,Value}]};
transform_row(#view_row{key=Key, id=_Id, value=_Value, doc={error,Reason}}) ->
    {row, [{id,error}, {key,Key}, {value,Reason}]};
transform_row(#view_row{key=Key, id=Id, value=Value, doc=Doc}) ->
    {row, [{id,Id}, {key,Key}, {value,Value}, {doc,Doc}]}.

compare(_, _, A, A) -> true;
compare(fwd, <<"raw">>, A, B) -> A < B;
compare(rev, <<"raw">>, A, B) -> B < A;
compare(fwd, _, A, B) -> couch_ejson_compare:less_json(A, B);
compare(rev, _, A, B) -> couch_ejson_compare:less_json(B, A).

extract_view(Pid, ViewName, [], _ViewType) ->
    couch_log:error("missing_named_view ~p", [ViewName]),
    exit(Pid, kill),
    exit(missing_named_view);
extract_view(Pid, ViewName, [View|Rest], ViewType) ->
    case lists:member(ViewName, view_names(View, ViewType)) of
    true ->
        if ViewType == reduce ->
            {index_of(ViewName, view_names(View, reduce)), View};
        true ->
            View
        end;
    false ->
        extract_view(Pid, ViewName, Rest, ViewType)
    end.

view_names(View, Type) when Type == red_map; Type == reduce ->
    [Name || {Name, _} <- View#mrview.reduce_funs];
view_names(View, map) ->
    View#mrview.map_names.

index_of(X, List) ->
    index_of(X, List, 1).

index_of(_X, [], _I) ->
    not_found;
index_of(X, [X|_Rest], I) ->
    I;
index_of(X, [_|Rest], I) ->
    index_of(X, Rest, I+1).

get_shards(Db, #mrargs{} = Args) ->
    DbPartitioned = fabric_util:is_partitioned(Db),
    Partition = couch_mrview_util:get_extra(Args, partition),
    if DbPartitioned orelse Partition == undefined -> ok; true ->
        throw({bad_request, <<"partition specified on non-partitioned db">>})
    end,
    DbName = fabric:dbname(Db),
    % Decide which version of mem3:shards/1,2 or
    % mem3:ushards/1,2 to use for the current
    % request.
    case {Args#mrargs.stable, Partition} of
        {true, undefined} ->
            mem3:ushards(DbName);
        {true, Partition} ->
            mem3:ushards(DbName, couch_partition:shard_key(Partition));
        {false, undefined} ->
            mem3:shards(DbName);
        {false, Partition} ->
            mem3:shards(DbName, couch_partition:shard_key(Partition))
    end.

maybe_update_others(DbName, DDoc, ShardsInvolved, ViewName,
    #mrargs{update=lazy} = Args) ->
    ShardsNeedUpdated = mem3:shards(DbName) -- ShardsInvolved,
    lists:foreach(fun(#shard{node=Node, name=ShardName}) ->
        rpc:cast(Node, fabric_rpc, update_mrview, [ShardName, DDoc, ViewName, Args])
    end, ShardsNeedUpdated);
maybe_update_others(_DbName, _DDoc, _ShardsInvolved, _ViewName, _Args) ->
    ok.


-spec fix_skip_and_limit(#mrargs{}) -> {CoordArgs::#mrargs{}, WorkerArgs::#mrargs{}}.
fix_skip_and_limit(#mrargs{} = Args) ->
    {CoordArgs, WorkerArgs} = case couch_mrview_util:get_extra(Args, partition) of
        undefined ->
            #mrargs{skip=Skip, limit=Limit}=Args,
            {Args, Args#mrargs{skip=0, limit=Skip+Limit}};
        _Partition ->
            {Args#mrargs{skip=0}, Args}
    end,
    %% the coordinator needs to finalize each row, so make sure the shards don't
    {CoordArgs, remove_finalizer(WorkerArgs)}.


remove_finalizer(Args) ->
    couch_mrview_util:set_extra(Args, finalizer, null).
