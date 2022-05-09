%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
%% -*- coding: utf-8 -*-
%% @private
%% Automatically generated, do not edit
%% Generated by gpb_compile version 4.19.5
%% Version source: git

-module(ots_ts_cache).

-include("ots_ts_sql.hrl").

-behaviour(gen_server).

-export([ start/2
        , stop/1
        , get/2
        , get/6
        , put/2
        , format/2
        , format/5
        , get_key/4
        ]).

-export([ start_link/1
        , init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
    clean_interval :: integer(),
    cache_timeout  :: integer(),
    table          :: atom(),
    timer          :: timer:tref()
    }).

-define(TAB, ?MODULE).

start(ID, Opts) ->
    Spec = #{
        id => ID,
        start => {ots_ts_cache, start_link, [Opts]},
        restart => permanent,
        shutdown => brutal_kill,
        type => worker
    },
    ots_erl_sup:start_child(Spec).

stop(ID) ->
    ots_erl_sup:stop_child(ID).

get(CacheTab, Table, Measurement, DataSource, Tags, 'MUM_NORMAL') ->
    Key = get_key(Table, Measurement, DataSource, Tags),
    ots_ts_cache:get(CacheTab, Key);

get(_, _, _, _, _, _MUM_IGNORE) ->
    %% ignore mode will has no cache data
    {undefined, 0}.

get(CacheTab, Key) ->
    case ets:lookup(CacheTab, Key) of
        [{_, Cache, _}] ->
            {Key, Cache};
        [] ->
            {Key, 0}
    end.

put(CacheTable, Caches) ->
    io:format("update cache ~p~n", [Caches]),
    ets:insert(CacheTable, Caches).

format(Table, Measurement, DataSource, Tags, Cache) ->
    format(get_key(Table, Measurement, DataSource, Tags), Cache).

format(Key, Cache) ->
    {Key, Cache, erlang:system_time()}.

get_key(Table, Measurement, undefined, Tags) ->
    get_key(Table, Measurement, <<>>, Tags);

get_key(Table, Measurement, DataSource, Tags) ->
    <<
        Table/binary,
        "/n",
        Measurement/binary,
        "/n",
        DataSource/binary,
        "/n",
        Tags/binary
    >>.
%% -------------------------------------------------------------------------------------------------
%% gen server

start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

init(Opts) ->
    TableName = maps:get(cache_table, Opts),
    ets:new(TableName, [named_table, public, {write_concurrency, true}, {read_concurrency, true}]),
    CI = maps:get(clean_interval, Opts, ?CLEAN_CACHE_INTERVAL),
    CT = maps:get(cache_timeout, Opts, ?CACHE_TIMEOUT),
    {ok, TRef} = clean_timer(CI),
    Init = #state{
        clean_interval = CI,
        cache_timeout = CT,
        table = TableName,
        timer = TRef
    },
    {ok, Init}.

handle_call({update, CI, CT}, _From, State = #state{timer = OldTRef}) ->
    case {CI, CT} of
        {CI, CT} when is_integer(CI) andalso is_integer(CT) andalso CI > 0 andalso CT > 0 ->
            _ = timer:cancel(OldTRef),
            {ok, NRef} = clean_timer(CI),
            {reply, ok, State#state{clean_interval = CI, cache_timeout = CT, timer = NRef}};
        _ ->
            {reply, {error, bad_args}, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(clean_cache, State = #state{clean_interval = CI, cache_timeout = CT, table = Table}) ->
    Now = erlang:system_time(),
    %% Now - Cache Time > Timeout
    MatchSpec = [{{'_', '_', '$1'}, [], [{'>', {'-', Now, '$1'}, CT}]}],
    ets:select_delete(Table, MatchSpec),
    {ok, TRef} = clean_timer(CI),
    {noreply, State#state{timer = TRef}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

clean_timer(Interval) ->
    timer:send_after(Interval, self(), clean_cache).

