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

-define(TS_CLIENT_VERSION, '1.0').

-define(CLEAN_CACHE_INTERVAL, 10 *  60 * 1000).
-define(CACHE_TIMEOUT, 10 *  60 * 1000).

%% request failed is the first time, then retry 2 times, so the total retry times is 3.
-define(MAX_RETRY, 2).
-define(RETRY_TIMEOUT, 300).

%% time series
-define(CREATE_TIMESERIES_TABLE     , "/CreateTimeseriesTable").
-define(LIST_TIMESERIES_TABLE       , "/ListTimeseriesTable").
-define(UPDATE_TIMESERIES_TABLE     , "/UpdateTimeseriesTable").
-define(DESCRIBE_TIMESERIES_TABLE   , "/DescribeTimeseriesTable").
-define(DELETE_TIMESERIES_TABLE     , "/DeleteTimeseriesTable").
-define(PUT_TIMESERIES_DATA         , "/PutTimeseriesData").
-define(GET_TIMESERIES_DATA         , "/GetTimeseriesData").
-define(QUERY_TIMESERIES_META       , "/QueryTimeseriesMeta").
-define(UPDATE_TIMESERIES_META      , "/UpdateTimeseriesMeta").
-define(DELETE_TIMESERIES_META      , "/DeleteTimeseriesMeta").

% one of
%   ?CREATE_TIMESERIES_TABLE,
%   ?LIST_TIMESERIES_TABLE,
%   ?UPDATE_TIMESERIES_TABLE,
%   ?DESCRIBE_TIMESERIES_TABLE,
%   ?DELETE_TIMESERIES_TABLE,
%   ?PUT_TIMESERIES_DATA,
%   ?GET_TIMESERIES_DATA,
%   ?QUERY_TIMESERIES_META,
%   ?UPDATE_TIMESERIES_META,
%   ?DELETE_TIMESERIES_META
-type ts_api() :: list().

-record(ts_client, {
    pool                :: atom(),
    endpoint            :: binary(),
    instance            :: binary(),
    access_key          :: binary(),
    access_secret       :: binary(),
    cache_table         :: atom(),
    version             = ?TS_CLIENT_VERSION,
    state               = undefined
}).

-type ts_client() :: #ts_client{}.

-record(ts_request, {
    client                  :: ts_client(),
    api                     :: ts_api(),
    sql = undefined         :: map() | undefined,
    payload = <<>>          :: binary(),
    cache_keys = []         :: list(binary()),
    expect_resp = undefined :: atom(),
    request_id = <<>>       :: binary(),
    response_body = <<>>    :: binary(),
    response                :: term(),
    response_handler        :: function(),
    http_code = 0           :: integer(),
    retry_times = 0         :: integer(),
    retry_state = undefined :: term()
    }).

-type ts_request() :: #ts_request{}.

%% -*- coding: utf-8 -*-
%% Automatically generated, do not edit
%% Generated by gpb_compile version 4.19.5

-ifndef(ots_ts_sql).
-define(ots_ts_sql, true).

-define(ots_ts_sql_gpb_version, "4.19.5").

-ifndef('TIMESERIESTABLEOPTIONS_PB_H').
-define('TIMESERIESTABLEOPTIONS_PB_H', true).
-record('TimeseriesTableOptions',
        {time_to_live           :: integer() | undefined % = 1, optional, 32 bits
        }).
-endif.

-ifndef('TIMESERIESTABLEMETA_PB_H').
-define('TIMESERIESTABLEMETA_PB_H', true).
-record('TimeseriesTableMeta',
        {table_name             :: unicode:chardata() | undefined, % = 1, required
         table_options          :: ots_ts_sql:'TimeseriesTableOptions'() | undefined, % = 2, optional
         status                 :: unicode:chardata() | undefined % = 3, optional
        }).
-endif.

-ifndef('CREATETIMESERIESTABLEREQUEST_PB_H').
-define('CREATETIMESERIESTABLEREQUEST_PB_H', true).
-record('CreateTimeseriesTableRequest',
        {table_meta             :: ots_ts_sql:'TimeseriesTableMeta'() | undefined % = 1, required
        }).
-endif.

-ifndef('CREATETIMESERIESTABLERESPONSE_PB_H').
-define('CREATETIMESERIESTABLERESPONSE_PB_H', true).
-record('CreateTimeseriesTableResponse',
        {
        }).
-endif.

-ifndef('LISTTIMESERIESTABLEREQUEST_PB_H').
-define('LISTTIMESERIESTABLEREQUEST_PB_H', true).
-record('ListTimeseriesTableRequest',
        {
        }).
-endif.

-ifndef('LISTTIMESERIESTABLERESPONSE_PB_H').
-define('LISTTIMESERIESTABLERESPONSE_PB_H', true).
-record('ListTimeseriesTableResponse',
        {table_metas = []       :: [ots_ts_sql:'TimeseriesTableMeta'()] | undefined % = 1, repeated
        }).
-endif.

-ifndef('DELETETIMESERIESTABLEREQUEST_PB_H').
-define('DELETETIMESERIESTABLEREQUEST_PB_H', true).
-record('DeleteTimeseriesTableRequest',
        {table_name             :: unicode:chardata() | undefined % = 1, required
        }).
-endif.

-ifndef('DELETETIMESERIESTABLERESPONSE_PB_H').
-define('DELETETIMESERIESTABLERESPONSE_PB_H', true).
-record('DeleteTimeseriesTableResponse',
        {
        }).
-endif.

-ifndef('UPDATETIMESERIESTABLEREQUEST_PB_H').
-define('UPDATETIMESERIESTABLEREQUEST_PB_H', true).
-record('UpdateTimeseriesTableRequest',
        {table_name             :: unicode:chardata() | undefined, % = 1, required
         table_options          :: ots_ts_sql:'TimeseriesTableOptions'() | undefined % = 2, optional
        }).
-endif.

-ifndef('UPDATETIMESERIESTABLERESPONSE_PB_H').
-define('UPDATETIMESERIESTABLERESPONSE_PB_H', true).
-record('UpdateTimeseriesTableResponse',
        {
        }).
-endif.

-ifndef('DESCRIBETIMESERIESTABLEREQUEST_PB_H').
-define('DESCRIBETIMESERIESTABLEREQUEST_PB_H', true).
-record('DescribeTimeseriesTableRequest',
        {table_name             :: unicode:chardata() | undefined % = 1, required
        }).
-endif.

-ifndef('DESCRIBETIMESERIESTABLERESPONSE_PB_H').
-define('DESCRIBETIMESERIESTABLERESPONSE_PB_H', true).
-record('DescribeTimeseriesTableResponse',
        {table_meta             :: ots_ts_sql:'TimeseriesTableMeta'() | undefined % = 1, required
        }).
-endif.

-ifndef('METAQUERYCONDITION_PB_H').
-define('METAQUERYCONDITION_PB_H', true).
-record('MetaQueryCondition',
        {type                   :: 'COMPOSITE_CONDITION' | 'MEASUREMENT_CONDITION' | 'SOURCE_CONDITION' | 'TAG_CONDITION' | 'UPDATE_TIME_CONDITION' | 'ATTRIBUTE_CONDITION' | integer() | undefined, % = 1, required, enum MetaQueryConditionType
         proto_data             :: iodata() | undefined % = 2, required
        }).
-endif.

-ifndef('METAQUERYCOMPOSITECONDITION_PB_H').
-define('METAQUERYCOMPOSITECONDITION_PB_H', true).
-record('MetaQueryCompositeCondition',
        {op                     :: 'OP_AND' | 'OP_OR' | 'OP_NOT' | integer() | undefined, % = 1, required, enum MetaQueryCompositeOperator
         sub_conditions = []    :: [ots_ts_sql:'MetaQueryCondition'()] | undefined % = 2, repeated
        }).
-endif.

-ifndef('METAQUERYMEASUREMENTCONDITION_PB_H').
-define('METAQUERYMEASUREMENTCONDITION_PB_H', true).
-record('MetaQueryMeasurementCondition',
        {op                     :: 'OP_EQUAL' | 'OP_GREATER_THAN' | 'OP_GREATER_EQUAL' | 'OP_LESS_THAN' | 'OP_LESS_EQUAL' | 'OP_PREFIX' | integer() | undefined, % = 1, required, enum MetaQuerySingleOperator
         value                  :: unicode:chardata() | undefined % = 2, required
        }).
-endif.

-ifndef('METAQUERYSOURCECONDITION_PB_H').
-define('METAQUERYSOURCECONDITION_PB_H', true).
-record('MetaQuerySourceCondition',
        {op                     :: 'OP_EQUAL' | 'OP_GREATER_THAN' | 'OP_GREATER_EQUAL' | 'OP_LESS_THAN' | 'OP_LESS_EQUAL' | 'OP_PREFIX' | integer() | undefined, % = 1, required, enum MetaQuerySingleOperator
         value                  :: unicode:chardata() | undefined % = 2, required
        }).
-endif.

-ifndef('METAQUERYTAGCONDITION_PB_H').
-define('METAQUERYTAGCONDITION_PB_H', true).
-record('MetaQueryTagCondition',
        {op                     :: 'OP_EQUAL' | 'OP_GREATER_THAN' | 'OP_GREATER_EQUAL' | 'OP_LESS_THAN' | 'OP_LESS_EQUAL' | 'OP_PREFIX' | integer() | undefined, % = 1, required, enum MetaQuerySingleOperator
         tag_name               :: unicode:chardata() | undefined, % = 2, required
         value                  :: unicode:chardata() | undefined % = 3, required
        }).
-endif.

-ifndef('METAQUERYATTRIBUTECONDITION_PB_H').
-define('METAQUERYATTRIBUTECONDITION_PB_H', true).
-record('MetaQueryAttributeCondition',
        {op                     :: 'OP_EQUAL' | 'OP_GREATER_THAN' | 'OP_GREATER_EQUAL' | 'OP_LESS_THAN' | 'OP_LESS_EQUAL' | 'OP_PREFIX' | integer() | undefined, % = 1, required, enum MetaQuerySingleOperator
         attr_name              :: unicode:chardata() | undefined, % = 2, required
         value                  :: unicode:chardata() | undefined % = 3, required
        }).
-endif.

-ifndef('METAQUERYUPDATETIMECONDITION_PB_H').
-define('METAQUERYUPDATETIMECONDITION_PB_H', true).
-record('MetaQueryUpdateTimeCondition',
        {op                     :: 'OP_EQUAL' | 'OP_GREATER_THAN' | 'OP_GREATER_EQUAL' | 'OP_LESS_THAN' | 'OP_LESS_EQUAL' | 'OP_PREFIX' | integer() | undefined, % = 1, required, enum MetaQuerySingleOperator
         value                  :: integer() | undefined % = 2, required, 64 bits
        }).
-endif.

-ifndef('TIMESERIESKEY_PB_H').
-define('TIMESERIESKEY_PB_H', true).
-record('TimeseriesKey',
        {measurement            :: unicode:chardata() | undefined, % = 1, required
         source                 :: unicode:chardata() | undefined, % = 2, required
         tags                   :: unicode:chardata() | undefined % = 3, required
        }).
-endif.

-ifndef('TIMESERIESMETA_PB_H').
-define('TIMESERIESMETA_PB_H', true).
-record('TimeseriesMeta',
        {time_series_key        :: ots_ts_sql:'TimeseriesKey'() | undefined, % = 1, required
         attributes             :: unicode:chardata() | undefined, % = 2, optional
         update_time            :: integer() | undefined % = 3, optional, 64 bits
        }).
-endif.

-ifndef('QUERYTIMESERIESMETAREQUEST_PB_H').
-define('QUERYTIMESERIESMETAREQUEST_PB_H', true).
-record('QueryTimeseriesMetaRequest',
        {table_name             :: unicode:chardata() | undefined, % = 1, required
         condition              :: ots_ts_sql:'MetaQueryCondition'() | undefined, % = 2, optional
         get_total_hit          :: boolean() | 0 | 1 | undefined, % = 3, optional
         token                  :: iodata() | undefined, % = 4, optional
         limit                  :: integer() | undefined % = 5, optional, 32 bits
        }).
-endif.

-ifndef('QUERYTIMESERIESMETARESPONSE_PB_H').
-define('QUERYTIMESERIESMETARESPONSE_PB_H', true).
-record('QueryTimeseriesMetaResponse',
        {timeseries_metas = []  :: [ots_ts_sql:'TimeseriesMeta'()] | undefined, % = 1, repeated
         total_hit              :: integer() | undefined, % = 2, optional, 64 bits
         next_token             :: iodata() | undefined % = 3, optional
        }).
-endif.

-ifndef('TIMESERIESROWS_PB_H').
-define('TIMESERIESROWS_PB_H', true).
-record('TimeseriesRows',
        {type                   :: 'RST_FLAT_BUFFER' | 'RST_PLAIN_BUFFER' | 'RST_PROTO_BUFFER' | integer() | undefined, % = 1, required, enum RowsSerializeType
         rows_data              :: iodata() | undefined, % = 2, required
         flatbuffer_crc32c      :: integer() | undefined % = 3, optional, 32 bits
        }).
-endif.

-ifndef('PUTTIMESERIESDATAREQUEST_PB_H').
-define('PUTTIMESERIESDATAREQUEST_PB_H', true).
-record('PutTimeseriesDataRequest',
        {table_name             :: unicode:chardata() | undefined, % = 1, required
         rows_data              :: ots_ts_sql:'TimeseriesRows'() | undefined, % = 2, required
         meta_update_mode       :: 'MUM_NORMAL' | 'MUM_IGNORE' | integer() | undefined % = 3, optional, enum MetaUpdateMode
        }).
-endif.

-ifndef('FAILEDROWINFO_PB_H').
-define('FAILEDROWINFO_PB_H', true).
-record('FailedRowInfo',
        {row_index              :: integer() | undefined, % = 1, required, 32 bits
         error_code             :: unicode:chardata() | undefined, % = 2, optional
         error_message          :: unicode:chardata() | undefined % = 3, optional
        }).
-endif.

-ifndef('METAUPDATESTATUS_PB_H').
-define('METAUPDATESTATUS_PB_H', true).
-record('MetaUpdateStatus',
        {row_ids = []           :: [non_neg_integer()] | undefined, % = 1, repeated, 32 bits
         meta_update_times = [] :: [non_neg_integer()] | undefined % = 2, repeated, 32 bits
        }).
-endif.

-ifndef('PUTTIMESERIESDATARESPONSE_PB_H').
-define('PUTTIMESERIESDATARESPONSE_PB_H', true).
-record('PutTimeseriesDataResponse',
        {failed_rows = []       :: [ots_ts_sql:'FailedRowInfo'()] | undefined, % = 1, repeated
         meta_update_status     :: ots_ts_sql:'MetaUpdateStatus'() | undefined % = 2, optional
        }).
-endif.

-ifndef('GETTIMESERIESDATAREQUEST_PB_H').
-define('GETTIMESERIESDATAREQUEST_PB_H', true).
-record('GetTimeseriesDataRequest',
        {table_name             :: unicode:chardata() | undefined, % = 1, required
         time_series_key        :: ots_ts_sql:'TimeseriesKey'() | undefined, % = 2, required
         begin_time             :: integer() | undefined, % = 3, optional, 64 bits
         end_time               :: integer() | undefined, % = 4, optional, 64 bits
         specific_time          :: integer() | undefined, % = 5, optional, 64 bits
         token                  :: iodata() | undefined, % = 6, optional
         limit                  :: integer() | undefined % = 7, optional, 32 bits
        }).
-endif.

-ifndef('GETTIMESERIESDATARESPONSE_PB_H').
-define('GETTIMESERIESDATARESPONSE_PB_H', true).
-record('GetTimeseriesDataResponse',
        {rows_data              :: iodata() | undefined, % = 1, required
         next_token             :: iodata() | undefined % = 2, optional
        }).
-endif.

-ifndef('UPDATETIMESERIESMETAREQUEST_PB_H').
-define('UPDATETIMESERIESMETAREQUEST_PB_H', true).
-record('UpdateTimeseriesMetaRequest',
        {table_name             :: unicode:chardata() | undefined, % = 1, required
         timeseries_meta = []   :: [ots_ts_sql:'TimeseriesMeta'()] | undefined % = 2, repeated
        }).
-endif.

-ifndef('UPDATETIMESERIESMETARESPONSE_PB_H').
-define('UPDATETIMESERIESMETARESPONSE_PB_H', true).
-record('UpdateTimeseriesMetaResponse',
        {failed_rows = []       :: [ots_ts_sql:'FailedRowInfo'()] | undefined % = 1, repeated
        }).
-endif.

-ifndef('DELETETIMESERIESMETAREQUEST_PB_H').
-define('DELETETIMESERIESMETAREQUEST_PB_H', true).
-record('DeleteTimeseriesMetaRequest',
        {table_name             :: unicode:chardata() | undefined, % = 1, required
         timeseries_key = []    :: [ots_ts_sql:'TimeseriesKey'()] | undefined % = 2, repeated
        }).
-endif.

-ifndef('DELETETIMESERIESMETARESPONSE_PB_H').
-define('DELETETIMESERIESMETARESPONSE_PB_H', true).
-record('DeleteTimeseriesMetaResponse',
        {failed_rows = []       :: [ots_ts_sql:'FailedRowInfo'()] | undefined % = 1, repeated
        }).
-endif.

-ifndef('TIMESERIESFIELD_PB_H').
-define('TIMESERIESFIELD_PB_H', true).
-record('TimeseriesField',
        {field_name             :: unicode:chardata() | undefined, % = 1, optional
         value_int              :: integer() | undefined, % = 2, optional, 64 bits
         value_string           :: unicode:chardata() | undefined, % = 3, optional
         value_bool             :: boolean() | 0 | 1 | undefined, % = 4, optional
         value_double           :: float() | integer() | infinity | '-infinity' | nan | undefined, % = 5, optional
         value_binary           :: iodata() | undefined % = 6, optional
        }).
-endif.

-ifndef('TIMESERIESROW_PB_H').
-define('TIMESERIESROW_PB_H', true).
-record('TimeseriesRow',
        {timeseries_key         :: ots_ts_sql:'TimeseriesKey'() | undefined, % = 1, optional
         time                   :: integer() | undefined, % = 2, optional, 64 bits
         fields = []            :: [ots_ts_sql:'TimeseriesField'()] | undefined, % = 3, repeated
         meta_cache_update_time :: non_neg_integer() | undefined % = 4, optional, 32 bits
        }).
-endif.

-ifndef('TIMESERIESPBROWS_PB_H').
-define('TIMESERIESPBROWS_PB_H', true).
-record('TimeseriesPBRows',
        {rows = []              :: [ots_ts_sql:'TimeseriesRow'()] | undefined % = 1, repeated
        }).
-endif.

-ifndef('ERRORRESPONSE_PB_H').
-define('ERRORRESPONSE_PB_H', true).
-record('ErrorResponse',
        {code                   :: unicode:chardata() | undefined, % = 1, required
         message                :: unicode:chardata() | undefined % = 2, optional
        }).
-endif.

-endif.
