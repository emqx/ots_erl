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
request(Client, SQL, Type) ->
    {ReqTransform, Handler} = transform_request(Type),
    Req = ReqTransform(Client, SQL),
    Resp = http_request(Req),
    %% requested, so retry time is 1
    handler_response(Req, Resp, Handler, 1, #{}).

handler_response(Req, Resp, Handler, RetryTime, RetryLoop) when RetryTime < ?MAX_RETRY ->
    case Handler(Req, Resp, RetryLoop) of
        {ok, Return} ->
            {ok, Return};
        {retry, RetryReq, RetryLoop} ->
            timer:sleep(retry_timeout(RetryTime)),
            NResp = http_request(Req),
            handler_response(RetryReq, NResp, Handler, RetryTime + 1, RetryLoop);
        {error, R} ->
            {error, R}
    end;
handler_response(_Req, _Resp, _Handler, _MAX_RETRY, RetryLoop) ->
    {error, RetryLoop}.

retry_timeout(RetryTime) when RetryTime > 0 -> RetryTime * retry_timeout(RetryTime - 1);
retry_timeout(0) -> ?RETRY_TIMEOUT.

transform_request(Type) ->
    EncoderMap = #{
        create_table   => {fun create_table_req/2,   fun create_table_resp/3},
        list_tables    => {fun list_tables_req/2,    fun list_tables_resp/3},
        delete_table   => {fun delete_table_req/2,   fun delete_table_resp/3},
        update_table   => {fun update_table_req/2,   fun update_table_resp/3},
        describe_table => {fun describe_table_req/2, fun describe_table_resp/3},
        query_meta     => {fun query_meta_req/2,     fun query_meta_resp/3},
        put            => {fun put_req/2,            fun put_resp/3},
        get            => {fun get_req/2,            fun get_resp/3},
        update_meta    => {fun update_meta_req/2,    fun update_meta_resp/3},
        delete_meta    => {fun delete_meta_req/2,    fun delete_meta_resp/3}
    },
    maps:get(Type, EncoderMap).

%% -------------------------------------------------------------------------------------------------
%% http request
http_request(#ts_request{client = Client, payload = Payload, api = API}) ->
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
        {ok, 200, RespHeaders, ResponseBody} ->
            ID = proplists:get_value(<<"x-ots-requestid">>, RespHeaders),
            {ok, #{request_id => ID, body => ResponseBody}};
        {ok, StatusCode, RespHeaders, ResponseBody} ->
            ID = proplists:get_value(<<"x-ots-requestid">>, RespHeaders),
            {error, #{code => StatusCode, request_id => ID, body => ResponseBody}};
        {error, Reason} ->
            {error, Reason}
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
    StringToSign = [
        API,
        "\nPOST\n\n"
    ],
    SignData = list_to_binary(
        [StringToSign | [[string:trim(H), ":", string:trim(V), "\n"] ||{H, V} <- HeadersPart1]]),
    base64:encode(crypto:mac(hmac, sha, AccessSecret, SignData)).

%% -------------------------------------------------------------------------------------------------
%% list tables
list_tables_req(Client, _SQL) ->
    #ts_request{
        client = Client,
        api = ?LIST_TIMESERIES_TABLE
    }.
%% TODO: check response
create_table_resp(_Req, Resp, _RetryLoop) -> Resp.
list_tables_resp(_Req, Resp, _RetryLoop) -> Resp.
delete_table_resp(_Req, Resp, _RetryLoop) -> Resp.
update_table_resp(_Req, Resp, _RetryLoop) -> Resp.
describe_table_resp(_Req, Resp, _RetryLoop) -> Resp.
query_meta_resp(_Req, Resp, _RetryLoop) -> Resp.
get_resp(_Req, Resp, _RetryLoop) -> Resp.
update_meta_resp(_Req, Resp, _RetryLoop) -> Resp.
delete_meta_resp(_Req, Resp, _RetryLoop) -> Resp.

%% -------------------------------------------------------------------------------------------------
%% create table
create_table_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?CREATE_TIMESERIES_TABLE
    }.
%% -------------------------------------------------------------------------------------------------
%% delete table
delete_table_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?DELETE_TIMESERIES_TABLE
    }.

%% -------------------------------------------------------------------------------------------------
%% update table
update_table_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?UPDATE_TIMESERIES_TABLE
    }.

%% -------------------------------------------------------------------------------------------------
%% describe table
describe_table_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?DESCRIBE_TIMESERIES_TABLE
    }.

%% -------------------------------------------------------------------------------------------------
%% query meta
query_meta_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?QUERY_TIMESERIES_META
    }.

%% -------------------------------------------------------------------------------------------------
%% put row
put_req(Client, SQL) ->
    CacheTab = cache_table(Client),
    TableName = maps:get(table_name, SQL),
    MetaUpdateMode = maps:get(meta_update_mode, SQL, 'MUM_IGNORE'),
    Rows = rows_time(maps:get(rows_data, SQL, [])),
    {RowsBinaryData, CacheKeys} = encode_pb_rows(CacheTab, TableName, Rows, MetaUpdateMode),
    Request = #'PutTimeseriesDataRequest'{
        table_name = TableName,
        rows_data = #'TimeseriesRows'{
            type = 'RST_PROTO_BUFFER',
            rows_data = RowsBinaryData,
            flatbuffer_crc32c = 0
        },
        meta_update_mode = MetaUpdateMode
    },
    #ts_request{
        client     = Client,
        api        = ?PUT_TIMESERIES_DATA,
        sql        = SQL#{rows_data => Rows},
        payload    = ots_ts_sql:encode_msg(Request),
        cache_keys = CacheKeys
    }.

put_resp(Req = #ts_request{client = Client, cache_keys = CacheKeys},
         _Resp = {ok, #{request_id := RequestID, body := Body}},
         RetryLoop) ->
    #'PutTimeseriesDataResponse'{
        meta_update_status = #'MetaUpdateStatus'{
            'row_ids' = IDs,
            'meta_update_times' = Times
        },
        failed_rows = FailedRows
    } = ots_ts_sql:decode_msg(Body, 'PutTimeseriesDataResponse'),
    CacheTab = cache_table(Client),
    ok = update_cache(CacheTab, CacheKeys, IDs, Times),
    Return = #{response_id => RequestID},
    case FailedRows of
        [] ->
            {ok, Return};
        _ ->
            retry_put(Req, FailedRows, Return, RetryLoop)
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

retry_put(#ts_request{sql = SQL, client = Client}, FailedRows, Return, RetryLoop) ->
    Rows = maps:get(rows_data, SQL, []),
    LastFailed = maps:get(failed, RetryLoop, []),
    case retry_aggregate(Rows, FailedRows, [], []) of
        {error, R} ->
            {error, R};
        {[], Failed} ->
            {error, Return#{failed => lists:append(Failed, LastFailed)}};
        {RetryRow, []} ->
            RetrySQL = SQL#{rows_data => RetryRow},
            RetryReq = put_req(Client, RetrySQL),
            {retry, RetryReq, Return#{failed => LastFailed}};
        {RetryRow, Failed} ->
            RetrySQL = SQL#{rows_data => RetryRow},
            RetryReq = put_req(Client, RetrySQL),
            {retry, RetryReq, Return#{failed => lists:append(Failed, LastFailed)}}
    end.

retry_aggregate(_Rows,[], Retry, Failed) ->
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
            NoRetryCodes = [<<"OTSParameterInvalid">>, <<"OTSAuthFailed">>],
            case lists:member(Code, NoRetryCodes)  of
                true ->
                    retry_aggregate(Rows, FailedRows, Retry,
                        [#{row => Row, error_code => Code, error_message => Message} | Failed]);
                false ->
                    retry_aggregate(Rows, FailedRows, [Row | Retry], [Failed])
            end;
        false ->
            {error, {bad_response, index_out_of_range}}
    end.

%% ensure row has time field.
rows_time(Rows) ->
    [begin
        case Row of
            #{time := T} when is_integer(T) ->
                Row;
            _ ->
                Row#{time => erlang:system_time(microsecond)}
        end
    end || Row <- Rows].

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

%% -------------------------------------------------------------------------------------------------
%% get row
get_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?GET_TIMESERIES_DATA
    }.

%% -------------------------------------------------------------------------------------------------
%% update meta
update_meta_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?UPDATE_TIMESERIES_META
    }.

%% -------------------------------------------------------------------------------------------------
%% delete meta
delete_meta_req(Client, SQL) ->
    #ts_request{
        client = Client,
        sql = SQL,
        api = ?DELETE_TIMESERIES_META
    }.
