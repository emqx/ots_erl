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

-module(ots_ts_client).

-include("ots_ts_sql.hrl").

-export([ start/1
        , stop/1
        ]).

-export([ create_table/2
        , list_tables/1
        , delete_table/2
        , update_table/2
        , describe_table/2
        , query_meta/2
        , put/2
        , get/2
        , update_meta/2
        , delete_meta/2
        ]).

-export([ pool/1
        , endpoint/1
        , instance/1
        , access_key/1
        , access_secret/1
        , cache_table/1
        ]).

start(Opts) -> do_start(Opts).
stop(Client) -> do_stop(Client).

create_table(Client, SQL)   -> request(Client, SQL, ?FUNCTION_NAME).
list_tables(Client)         -> request(Client, #{}, ?FUNCTION_NAME).
delete_table(Client, SQL)   -> request(Client, SQL, ?FUNCTION_NAME).
update_table(Client, SQL)   -> request(Client, SQL, ?FUNCTION_NAME).
describe_table(Client, SQL) -> request(Client, SQL, ?FUNCTION_NAME).
query_meta(Client, SQL)     -> request(Client, SQL, ?FUNCTION_NAME).
put(Client, SQL)            -> request(Client, SQL, ?FUNCTION_NAME).
get(Client, SQL)            -> request(Client, SQL, ?FUNCTION_NAME).
update_meta(Client, SQL)    -> request(Client, SQL, ?FUNCTION_NAME).
delete_meta(Client, SQL)    -> request(Client, SQL, ?FUNCTION_NAME).

%% -------------------------------------------------------------------------------------------------
%% client info
pool(#ts_client{pool = P}) -> P.
endpoint(#ts_client{endpoint = E}) -> E.
instance(#ts_client{instance = I}) -> I.
access_key(#ts_client{access_key = K}) -> K.
access_secret(#ts_client{access_secret = S}) -> S.
cache_table(#ts_client{cache_table = C}) -> C.

%% -------------------------------------------------------------------------------------------------
%% internal
do_start(Opts) ->
    Pool = proplists:get_value(pool, Opts),
    PoolBin = atom_to_binary(Pool, utf8),
    CacheTable = binary_to_atom(<<"ts_cache_", PoolBin/binary>>, utf8),
    Client = #ts_client{
        pool          = Pool,
        endpoint      = proplists:get_value(endpoint, Opts),
        instance      = proplists:get_value(instance, Opts),
        access_key    = proplists:get_value(access_key, Opts),
        access_secret = proplists:get_value(access_secret, Opts),
        cache_table   = CacheTable
    },
    PoolSize = proplists:get_value(pool_size, Opts, 8),
    PoolOptions = [
        {pool_size, PoolSize},
        {timeout, 150000},
        {max_connections, PoolSize * 8}
    ],
    case hackney_pool:start_pool(Pool, PoolOptions) of
        ok ->
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

do_stop(Client) ->
    hackney_pool:stop_pool(pool(Client)),
    ots_ts_cache:stop(cache_table(Client)).

request(Client, SQL, Type) ->
    ReqTransform = transform_request(Type),
    Req = ReqTransform(Client, SQL),
    NReq = http_request(Req#ts_request{retry_times = 0}),
    handler_response(NReq).

handler_response(Req = #ts_request{retry_times = RT0, response_handler = Handler})
  when RT0 < ?MAX_RETRY ->
    case Handler(Req) of
        {ok, Return} ->
            {ok, Return};
        {retry, RetryReq} ->
            %% sleep & retry request
            RT = RT0 + 1,
            timer:sleep(retry_timeout(RT)),
            NReq = http_request(RetryReq#ts_request{retry_times = RT}),
            handler_response(NReq);
        {error, R} when is_map(R) ->
            {error, R#{retry_times => RT0}};
        {error, R} ->
            {error, #{reason=> R, retry_times => RT0}}
    end;
handler_response(#ts_request{retry_state = RS, retry_times = RT}) ->
    {error, RS#{reason => retry_timeout, retry_times => RT}}.

retry_timeout(RetryTime) when RetryTime > 0 -> RetryTime * retry_timeout(RetryTime - 1);
retry_timeout(0) -> ?RETRY_TIMEOUT.

transform_request(Type) ->
    EncoderMap = #{
        create_table   => fun create_table_req/2,
        list_tables    => fun list_tables_req/2,
        delete_table   => fun delete_table_req/2,
        update_table   => fun update_table_req/2,
        describe_table => fun describe_table_req/2,
        query_meta     => fun query_meta_req/2,
        put            => fun put_req/2,
        get            => fun get_req/2,
        update_meta    => fun update_meta_req/2,
        delete_meta    => fun delete_meta_req/2
    },
    maps:get(Type, EncoderMap).

%% -------------------------------------------------------------------------------------------------
%% http request
http_request(Req = #ts_request{client = Client,
                               api = API,
                               payload = Payload,
                               expect_resp = ExpectRespType}) ->
    Headers = ts_headers(Client, API, Payload),
    Pool = pool(Client),
    Options = [
        {pool, Pool},
        {connect_timeout, 10000},
        {recv_timeout, 30000},
        {follow_redirectm, true},
        {max_redirect, 5},
        with_body
    ],
    Url = list_to_binary([endpoint(Client), API]),
    case hackney:request(post, Url, Headers, Payload, Options) of
        {ok, StatusCode, RespHeaders, ResponseBody} ->
            ID = proplists:get_value(<<"x-ots-requestid">>, RespHeaders),
            Response = decode_resp(ResponseBody, ExpectRespType),
            Req#ts_request{
                http_code = StatusCode,
                response_body = ResponseBody,
                response = Response,
                request_id = ID
            };
        {error, Reason} ->
            %% no more action, return error
            {error, Reason}
    end.

decode_resp(_Body, undefined) -> undefined;
decode_resp(Body, ResponseType) ->
    try ots_ts_sql:decode_msg(Body, ResponseType)
    catch _:_ ->
        decode_error_resp(Body)
    end.

decode_error_resp(Body) ->
    try ots_ts_sql:decode_msg(Body, 'ErrorResponse')
    catch _:_ ->
        unknow
    end.

ts_headers(Client, API, Body) ->
    %% Order by string name.
    %% [{header_a, _}, {header_b, _}, ....]
    %% If you don't know the rules of sorting, don't change it.
    HeadersPart1 = [
        {<<"x-ots-accesskeyid">>  , access_key(Client)},
        {<<"x-ots-apiversion">>   , <<"2015-12-31">>},
        {<<"x-ots-contentmd5">>   , base64:encode(erlang:md5(Body))},
        {<<"x-ots-date">>         , iso8601_now()},
        {<<"x-ots-instancename">> , instance(Client)}
    ],
    Sign = {<<"x-ots-signature">>, sign(API, access_secret(Client), HeadersPart1)},
    lists:append(HeadersPart1, [Sign]).

iso8601_now() ->
    {DatePart1, {H, M, S}} = calendar:universal_time(),
    iso8601:format({DatePart1, {H, M, S * 1.0}}).

sign(API, AccessSecret, HeadersPart1) ->
    StringToSign = [API, "\nPOST\n\n"],
    SignData = list_to_binary(
        [StringToSign | [[string:trim(H), ":", string:trim(V), "\n"] ||{H, V} <- HeadersPart1]]),
    base64:encode(crypto:mac(hmac, sha, AccessSecret, SignData)).

%% -------------------------------------------------------------------------------------------------
%% list tables
list_tables_req(Client, _SQL) ->
    #ts_request{
        client = Client,
        api = ?LIST_TIMESERIES_TABLE,
        expect_resp = 'ListTimeseriesTableResponse',
        response_handler = fun list_tables_resp/1
    }.

list_tables_resp(Req)-> format(Req).

%% -------------------------------------------------------------------------------------------------
%% create table
create_table_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?CREATE_TIMESERIES_TABLE,
        response_handler = fun create_table_resp/1
    }.
create_table_resp(Req) -> {ok, Req}.

%% -------------------------------------------------------------------------------------------------
%% delete table
delete_table_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?DELETE_TIMESERIES_TABLE,
        response_handler = fun delete_table_resp/1
    }.

delete_table_resp(Req) -> {ok, Req}.

%% -------------------------------------------------------------------------------------------------
%% update table
update_table_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?UPDATE_TIMESERIES_TABLE,
        response_handler = fun update_table_resp/1
    }.

update_table_resp(Req) -> {ok, Req}.

%% -------------------------------------------------------------------------------------------------
%% describe table
describe_table_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?DESCRIBE_TIMESERIES_TABLE,
        response_handler = fun describe_table_resp/1
    }.

describe_table_resp(Req) -> {ok, Req}.

%% -------------------------------------------------------------------------------------------------
%% query meta
query_meta_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?QUERY_TIMESERIES_META,
        response_handler = fun query_meta_resp/1
    }.

query_meta_resp(Req) -> {ok, Req}.

%% -------------------------------------------------------------------------------------------------
%% put row
put_req(Client, SQL) ->
    put_req(Client, SQL, #{}).

put_req(Client, SQL, RetryState) ->
    CacheTab = cache_table(Client),
    TableName = maps:get(table_name, SQL),
    MetaUpdateMode = maps:get(meta_update_mode, SQL, 'MUM_IGNORE'),
    %% ensure row has time field.
    Rows =
        [begin
            case Row of
                #{time := T} when is_integer(T) ->
                    Row;
                _ ->
                    Row#{time => erlang:system_time(microsecond)}
            end
        end || Row <- maps:get(rows_data, SQL, [])],
    {RowsBinaryData, CacheKeys} = encode_pb_rows(CacheTab, TableName, Rows, MetaUpdateMode),
    OTSRequest = #'PutTimeseriesDataRequest'{
        table_name = TableName,
        rows_data = #'TimeseriesRows'{
            type = 'RST_PROTO_BUFFER',
            rows_data = RowsBinaryData,
            flatbuffer_crc32c = 0
        },
        meta_update_mode = MetaUpdateMode
    },
    Handler =
        case MetaUpdateMode of
            'MUM_IGNORE' ->
                fun put_resp_ignore/1;
            _ ->
                fun put_resp/1
        end,
    #ts_request{
        client      = Client,
        api         = ?PUT_TIMESERIES_DATA,
        sql         = SQL#{rows_data => Rows},
        payload     = ots_ts_sql:encode_msg(OTSRequest),
        cache_keys  = CacheKeys,
        expect_resp = 'PutTimeseriesDataResponse',
        retry_state = RetryState,
        response_handler = Handler
    }.

put_resp_ignore(#ts_request{http_code = HCode, request_id = ID}) ->
    case HCode of
        200 ->
            {ok, []};
        _ ->
            {error, #{http_code => HCode, request_id => ID}}
    end.

put_resp(Req = #ts_request{response = Error = #'ErrorResponse'{code = Code},
                           request_id = ID,
                           retry_times = RT,
                           retry_state = RS
                          }) ->
    {_, ErrorFormat} = format(Error),
    RTLog = maps:get(retry_log, RS, #{}),
    NRTLog = #{
        RT => #{
            request_id => ID,
            reason => ErrorFormat
        }
    },
    RetryState = #{retry_log => maps:merge(RTLog, NRTLog)},
    case ?RETRY_CODE(Code) of
        true ->
            {retry, Req#ts_request{retry_state = RetryState}};
        false ->
            {error, RetryState}
    end;
put_resp(Req = #ts_request{ client = Client,
                            cache_keys = CacheKeys,
                            request_id = RequestID,
                            response = #'PutTimeseriesDataResponse'{
                                meta_update_status = MetaUpdateStatus,
                                failed_rows = FailedRows},
                            retry_state = RSate}) ->
    CacheTab = cache_table(Client),
    ok = update_cache(CacheTab, CacheKeys, MetaUpdateStatus),
    case is_empty_list(FailedRows) of
        true ->
            {ok, #{request_id => RequestID}};
        _ ->
            retry_put(Req, FailedRows, RSate)
    end.


update_cache(_CacheTab, _CacheKeys, undefined) -> ok;
update_cache(CacheTab,
             CacheKeys,
             #'MetaUpdateStatus'{'row_ids' = IDs,'meta_update_times' = Times}) ->
    update_cache(CacheTab, CacheKeys, IDs, Times).
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

retry_put(#ts_request{client = Client, sql = SQL, request_id = ID, retry_times = RT},
          FailedRowsIndex,
          RetryState) ->
    Rows = maps:get(rows_data, SQL, []),
    LastFailed = maps:get(failed, RetryState, []),
    RetryLog = maps:get(retry_log, RetryState, #{}),
    case retry_aggregate(Rows, FailedRowsIndex) of
        {error, R} ->
            NRTLog = #{
                RT => #{
                    request_id => ID,
                    reason => R
                }
            },
            NRetryState = #{
                request_id => ID,
                retry => [],
                failed => Rows,
                retry_log => maps:merge(RetryLog, NRTLog)
            },
            %% some error response from ots.
            {error, NRetryState};
        {[], Failed} ->
            %% no more retry, all failed.
            NRTLog = #{
                RT => #{
                    request_id => ID,
                    reason => Failed
                }
            },
            NRetryState = #{
                retry => [],
                request_id => ID,
                failed => lists:append(LastFailed, Failed),
                retry_log => maps:merge(RetryLog, NRTLog)
            },
            {error, NRetryState};
        {RetryRows, Failed} ->
            %% retry some. and failed the no retry rows.
            NRTLog = #{
                RT => #{
                    request_id => ID,
                    failed => Failed
                }
            },
            RetrySQL = SQL#{rows_data => RetryRows},
            NRetryState = #{
                retry => RetryRows,
                failed => lists:append(LastFailed, Failed),
                retry_log => maps:merge(RetryLog, NRTLog)
            },
            {retry, put_req(Client, RetrySQL, NRetryState)}
    end.

retry_aggregate(Rows, FailedRowsIndex) ->
    retry_aggregate(Rows, FailedRowsIndex, [], []).
retry_aggregate(_Rows, [], Retry, Failed) ->
    {Retry, Failed};
retry_aggregate(Rows,
          [#'FailedRowInfo'{row_index = Index, error_code = Code, error_message = Message} | FailedRows],
          Retry,
          Failed) ->
    %% Java index starts from 0, but Erlang index starts from 1.
    ErlangIndex = Index + 1,
    case 0 < ErlangIndex andalso ErlangIndex =< length(Rows) of
        true ->
            Row = lists:nth(ErlangIndex, Rows),
            case ?RETRY_CODE(Code) of
                false ->
                    FailedRowInfo = #{
                        row_index => ErlangIndex,
                        code => Code,
                        message => Message},
                    retry_aggregate(Rows, FailedRows, Retry, [FailedRowInfo | Failed]);
                true ->
                    retry_aggregate(Rows, FailedRows, [Row | Retry], [Failed])
            end;
        false ->
            {error, {bad_response, index_out_of_range}}
    end.

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
    {ots_ts_sql:encode_msg(BPRows), CacheKeys};

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
        time = maps:get(time, Row),
        fields = to_fields(maps:get(fields, Row, [])),
        meta_cache_update_time = MetaCache
    },
    {RowStruct, CacheKey}.

to_timeseries_key(Measurement, DataSource, Tags) ->
    #'TimeseriesKey'{
        measurement = Measurement,
        source = DataSource,
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

%% -------------------------------------------------------------------------------------------------
%% get row
get_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?GET_TIMESERIES_DATA,
        response_handler = fun get_resp/1
    }.

get_resp(Req) -> {ok, Req}.

%% -------------------------------------------------------------------------------------------------
%% update meta
update_meta_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?UPDATE_TIMESERIES_META,
        response_handler = fun update_meta_resp/1
    }.

update_meta_resp(Req) -> {ok, Req}.

%% -------------------------------------------------------------------------------------------------
%% delete meta
delete_meta_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?DELETE_TIMESERIES_META,
        response_handler = fun delete_meta_resp/1
    }.

delete_meta_resp(Req) -> {ok, Req}.

%% -------------------------------------------------------------------------------------------------
%% util

format(#ts_request{http_code = StatusCode, request_id = ID, response = Response}) ->
    case format(Response) of
        {ok, Format} ->
            {ok, Format};
        {error, R} when is_map(R) ->
            {error, R#{http_code => StatusCode, request_id => ID}}
    end;

format(#'ListTimeseriesTableResponse'{table_metas = TableMetas}) ->
    {ok,
        [#{table_name => TableName, status => Status, time_to_live => TimeToLive}
        ||
        #'TimeseriesTableMeta'{
                table_name = TableName,
                status = Status,
                table_options = #'TimeseriesTableOptions'{
                    time_to_live = TimeToLive}} <- TableMetas]};
format(#'ErrorResponse'{code = Code, message = Message}) ->
    {error, #{code => Code, message => Message}}.

is_empty_list(List) when is_list(List) -> erlang:length(List) == 0;
is_empty_list(_) -> true.

to_binary(Bin)  when is_binary(Bin) -> Bin;
to_binary(Num)  when is_number(Num) -> number_to_binary(Num);
to_binary(Atom) when is_atom(Atom)  -> atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List)  -> list_to_binary(List).

number_to_binary(Int) when is_integer(Int) ->
    integer_to_binary(Int);
number_to_binary(Float) when is_float(Float) ->
    float_to_binary(Float, [{decimals, 10}, compact]).
