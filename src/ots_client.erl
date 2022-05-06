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

-module(ots_client).

-include("ots.hrl").

-export([ start/1
        , list_ts_tables/1
        , write/2
        , stop/1
        ]).

-export([ pool/1
        , type/1
        , endpoint/1
        , instance/1
        , access_key/1
        , access_secret/1
        ]).

start(Opts) when is_map(Opts) -> start(maps:to_list(Opts));
start(Opts) when is_list(Opts) ->
    Client = #ots_client{
        pool          = proplists:get_value(pool, Opts),
        type          = proplists:get_value(type, Opts, ?OTS_CLIENT_TS),
        endpoint      = proplists:get_value(endpoint, Opts),
        instance      = proplists:get_value(instance, Opts),
        access_key    = proplists:get_value(access_key, Opts),
        access_secret = proplists:get_value(access_secret, Opts),
        cache_table   = cache_table_name(Opts)
    },
    do_start(Client, Opts).

stop(Client) ->
    Pool = pool(Client),
    hackney_pool:stop_pool(Pool),
    CacheTable = cache_table(Client),
    ots_ts_cache:stop(CacheTable).

list_ts_tables(Client) ->
    request(Client, ?FUNCTION_NAME, ?FUNCTION_NAME).

write(Client, Data) ->
    request(Client, ?FUNCTION_NAME, Data).

%% -------------------------------------------------------------------------------------------------
%% internal
cache_table_name(Opts) ->
    Pool = proplists:get_value(pool, Opts),
    PoolBin = atom_to_binary(Pool, utf8),
    binary_to_atom(<<"ots_cache_", PoolBin/binary>>, utf8).

do_start(Client, Opts) ->
    Pool = pool(Client),
    PoolSize = proplists:get_value(pool_size, Opts, 8),
    PoolOptions = [
        {pool_size, PoolSize},
        {timeout, 150000},
        {max_connections, PoolSize * 8}
    ],
    case hackney_pool:start_pool(Pool, PoolOptions) of
        ok ->
            CacheTable = cache_table(Client),
            CacheOpts = #{
                cache_table => CacheTable,
                clean_interval => proplists:get_value(clean_interval, Opts, ?CLEAN_CACHE_INTERVAL),
                cache_timeout => proplists:get_value(cache_timeout, Opts, ?CACHE_TIMEOUT)
            },
            {ok, _} = ots_ts_cache:start(CacheTable, CacheOpts),
            {ok, Client};
        Error ->
            Error
    end.

request(Client, Req, Data) ->
    case type(Client) of
        ?OTS_CLIENT_TS ->
            request_ts(Client, Req, Data);
        ?OTS_CLIENT_WC ->
            %% TODO: wide column support
            {error, not_support}
    end.

request_ts(Client, Req, Data) ->
    Endpoint = endpoint_ts(Req),
    CacheTable = cache_table(Client),
    {SQL, CacheKeys} = encode_ts(CacheTable, Data),
    case ots_http:request(Client, Endpoint, SQL) of
        {ok, Response, ID} ->
            response_ts(Client, Response, ID, Req, CacheKeys);
        {error, Reason} ->
            {error, Reason}
    end.

endpoint_ts(list_ts_tables) -> ?LIST_TIMESERIES_TABLE;
endpoint_ts(write) -> ?PUT_TIMESERIES_DATA.

response_ts(_, Response, ID, list_ts_tables, _) ->
    {ok, #{body => Response, id => ID}};
response_ts(_, <<>>, ID, _, _) ->
    {ok, #{body => <<>>, id => ID}};
response_ts(Client, Response, ID, write, CacheKeys) ->
    #'PutTimeseriesDataResponse'{
        meta_update_status = #'MetaUpdateStatus'{
            'row_ids' = IDs,
            'meta_update_times' = Times
            },
        failed_rows = FailedRows
    } = ots_sql:decode_msg(Response, 'PutTimeseriesDataResponse'),
    case FailedRows of
        [] ->
            CacheTab = cache_table(Client),
            ok = update_cache(CacheTab, CacheKeys, IDs, Times),
            {ok, #{id => ID}};
        _ ->
            {error, #{failed_rows => FailedRows, id => ID}}
    end.

update_cache(CacheTab, CacheKeys, IDs, Caches) ->
    case
        is_empty_list(CacheKeys) orelse
        is_empty_list(IDs) orelse
        is_empty_list(Caches)
    of
        true ->
            ok;
        false ->
            NewCaches = update_caches(CacheKeys, IDs, Caches, []),
            true = ots_ts_cache:put(CacheTab, NewCaches),
            ok
    end.

update_caches(_CacheKeys, IDs, Caches, NewCaches)
%% Maybe I should trust ots response, index & cache appear in pairs.
  when length(IDs) == 0 orelse length(Caches) == 0 ->
    NewCaches;
update_caches(CacheKeys, [Index | IDs], [Cache | Caches], NewCaches) ->
    %% Java list index start 0, but Erlang list index start 1.
    ErlangIndex = Index + 1,
    case erlang:length(CacheKeys) >= ErlangIndex andalso ErlangIndex > 0 of
        true ->
            Key = lists:nth(ErlangIndex, CacheKeys),
            NewCache = ots_ts_cache:format(Key, Cache),
            update_caches(CacheKeys, IDs, Caches, [NewCache | NewCaches]);
        false ->
            %% some error response, ignore bad cache info.
            NewCaches
    end.

is_empty_list(List) when is_list(List) -> erlang:length(List) == 0;
is_empty_list(_) -> true.

%% -------------------------------------------------------------------------------------------------
%% app internal

pool(#ots_client{pool = P}) -> P.
type(#ots_client{type = T}) -> T.
endpoint(#ots_client{endpoint = E}) -> E.
instance(#ots_client{instance = I}) -> I.
access_key(#ots_client{access_key = K}) -> K.
access_secret(#ots_client{access_secret = S}) -> S.
cache_table(#ots_client{cache_table = C}) -> C.

%% -------------------------------------------------------------------------------------------------
%% ts data transform
encode_ts(_, list_ts_tables) -> {<<>>, undefined};
encode_ts(CacheTable, Data) ->
    {Request, CacheKeys} = to_ots_data(CacheTable, Data),
    {ots_sql:encode_msg(Request), CacheKeys}.

to_ots_data(CacheTab, Data) ->
    TableName = maps:get(table_name, Data),
    MetaUpdateMode = maps:get(meta_update_mode, Data, 'MUM_IGNORE'),
    Rows = maps:get(rows_data, Data, []),
    {RowsBinaryData, CacheKeys} = encode_pb_rows(CacheTab, TableName, Rows, MetaUpdateMode),
    RowsData =
        #'TimeseriesRows'{
            type = 'RST_PROTO_BUFFER',
            rows_data = RowsBinaryData,
            flatbuffer_crc32c = 0
        },
    Request = #'PutTimeseriesDataRequest'{
        table_name = TableName,
        rows_data = RowsData,
        meta_update_mode = MetaUpdateMode
    },
    {Request, CacheKeys}.

encode_pb_rows(CacheTab, TableName, Rows, MetaUpdateMode) ->
    encode_pb_rows(CacheTab, TableName, Rows, MetaUpdateMode, {[], []}).

encode_pb_rows(_, _, [], MetaUpdateMode, {RL, KL}) ->
    CacheKeys =
        case MetaUpdateMode of
            'MUM_NORMAL' ->
                lists:reverse(KL);
            'MUM_IGNORE' ->
                undefined
        end,
    BPRows = #'TimeseriesPBRows'{rows = lists:reverse(RL)},
    {ots_sql:encode_msg(BPRows), CacheKeys};

encode_pb_rows(CacheTab, TableName, [Row | Rows], MetaUpdateMode, {RL, KL}) ->
    {R, K} = to_row_struct(CacheTab, TableName, Row, MetaUpdateMode),
    encode_pb_rows(CacheTab, TableName, Rows, MetaUpdateMode, {[R | RL], [K | KL]}).

to_row_struct(CacheTab, TableName, Row, MetaUpdateMode) ->
    Measurement = maps:get(measurement, Row),
    DataSource = maps:get(data_source, Row, <<>>),
    Tags = encode_tags(maps:get(tags, Row, [])),
    {CacheKey, MetaCache} =
        ots_ts_cache:get(CacheTab, TableName, Measurement, DataSource, Tags, MetaUpdateMode),
    RowStruct = #'TimeseriesRow'{
        timeseries_key = to_timeseries_key(Measurement, DataSource, Tags),
        time = row_time(Row),
        fields = to_fields(maps:get(fields, Row, [])),
        meta_cache_update_time = MetaCache
    },
    {RowStruct, CacheKey}.

row_time(#{time := Time}) -> Time;
row_time(_Row) -> erlang:system_time(microsecond).

to_timeseries_key(Measurement, DataSource, Tags) ->
    #'TimeseriesKey'{
        measurement = Measurement,
        data_source = DataSource,
        tags = Tags
    }.

encode_tags(Tags) when is_map(Tags) ->
    encode_tags(maps:to_list(Tags));
encode_tags(Tags) ->
    %% Tags must be sorted
    %% And as like ["a=tag1","b=tag2"]
    SortTags = lists:sort([{to_binary(K), to_binary(V)} || {K, V} <- Tags]),
    encode_tags(SortTags, []).

encode_tags([], Res) ->
    list_to_binary([<<"[">>, lists:reverse(Res), <<"]">>]);
encode_tags([{Key, Value}], Res) ->
    encode_tags([], [[<<"\"">>, Key, <<"=">>, Value, <<"\"">>] | Res]);
encode_tags([{Key, Value} | Tags], Res) ->
    encode_tags(Tags, [[<<"\"">>, Key, <<"=">>, Value, <<"\",">>] | Res]).

to_fields(Fields) when is_map(Fields) ->
    to_fields(maps:to_list(Fields));
to_fields(Fields) ->
    [to_field(Field) || Field <- Fields].

%% ensure binary key
to_field({Key, Value}) ->
    to_field(to_binary(Key), Value).

to_field(Key, Value) when is_integer(Value) ->
    #'TimeseriesField'{field_name = Key, value_int = Value};
to_field(Key, Value) when is_float(Value) ->
    #'TimeseriesField'{field_name = Key, value_double = Value};
to_field(Key, Value) when is_boolean(Value) ->
    #'TimeseriesField'{field_name = Key, value_bool = Value};
to_field(Key, Value) when is_atom(Value) ->
    #'TimeseriesField'{field_name = Key, value_string = atom_to_list(Value)};
to_field(Key, Value) when is_list(Value) ->
    #'TimeseriesField'{field_name = Key, value_string = Value};
to_field(Key, Value) when is_binary(Value) ->
    #'TimeseriesField'{field_name = Key, value_binary = Value};
to_field(Key, _Value) ->
    #'TimeseriesField'{field_name = Key}.

to_binary(Bin)  when is_binary(Bin) -> Bin;
to_binary(Num)  when is_number(Num) -> number_to_binary(Num);
to_binary(Atom) when is_atom(Atom)  -> atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List)  ->
    case io_lib:printable_list(List) of
        true -> list_to_binary(List);
        false -> emqx_json:encode(List)
    end.

number_to_binary(Int) when is_integer(Int) ->
    integer_to_binary(Int);
number_to_binary(Float) when is_float(Float) ->
    float_to_binary(Float, [{decimals, 10}, compact]).
