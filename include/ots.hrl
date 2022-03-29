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

-define(OTS_CLIENT_TS, time_series).
-define(OTS_CLIENT_WC, wide_column).

-type ots_client_type() :: ?OTS_CLIENT_TS | ?OTS_CLIENT_WC.

-record(ots_client, {
    pool                :: term(),
    type = time_series  :: ots_client_type(),
    endpoint            :: binary(),
    instance            :: binary(),
    access_key          :: binary(),
    access_secret       :: binary()
}).

%% table end point
-define(CREATE_TABLE                , "CreateTable").
-define(LIST_TABLE                  , "ListTable").
-define(DELETE_TABLE                , "DeleteTable").
-define(UPDATE_TABLE                , "UpdateTable").
-define(DESCRIBE_TABLE              , "DescribeTable").
-define(COMPUTE_SPLIT_POINTS_BY_SIZE, "ComputeSplitPointsBySize").

%% single row
-define(GET_ROW                     , "GetRow").
-define(PUT_ROW                     , "PutRow").
-define(UPDATE_ROW                  , "UpdateRow").
-define(DELETE_ROW                  , "DeleteRow").

%% batch row
-define(GET_RANGE                   , "GetRange").
-define(BATCH_GET_ROW               , "BatchGetRow").
-define(BATCH_WRITE_ROW             , "BatchWriteRow").

%% stream
-define(LIST_STREAM                 , "ListStream").
-define(DESCRIBE_STREAM             , "DescribeStream").
-define(GET_SHARD_ITERATOR          , "GetShardIterator").
-define(GET_STREAM_RECORD           , "GetStreamRecord").

%% index
-define(CREATE_INDEX                , "CreateIndex").
-define(DELETE_INDEX                , "DeleteIndex").

%% time series
-define(CREATE_TIMESERIES_TABLE     , "CreateTimeseriesTable").
-define(LIST_TIMESERIES_TABLE       , "ListTimeseriesTable").
-define(UPDATE_TIMESERIES_TABLE     , "UpdateTimeseriesTable").
-define(DESCRIBE_TIMESERIES_TABLE   , "DescribeTimeseriesTable").
-define(DELETE_TIMESERIES_TABLE     , "DeleteTimeseriesTable").
-define(PUT_TIMESERIES_DATA         , "PutTimeseriesData").
-define(GET_TIMESERIES_DATA         , "GetTimeseriesData").
-define(QUERY_TIMESERIES_META       , "QueryTimeseriesMeta").
-define(UPDATE_TIMESERIES_META      , "UpdateTimeseriesMeta").
-define(DELETE_TIMESERIES_META      , "DeleteTimeseriesMeta").

-type ots_key()     :: atom()
                     | string()
                     | binary().

-define(OTS_LONG,    1).
-define(OTS_BOOLEAN, 2).
-define(OTS_DOUBLE,  3).
-define(OTS_STRING,  4).
-define(OTS_BINARY,  5).

%% some names: measurement, source ,table
-type table()       :: ots_key().

-type tag_key()     :: ots_key().

-type tag_value()   :: atom()
                     | string()
                     | binary()
                     | integer()
                     | float()
                     | boolean().

-type tag()         :: {tag_key(), tag_value()}.

-type tags()        :: [tag()] | map().

%% by microsecond
-type time()        :: pos_integer() | undefined.

-type field_type()  :: ?OTS_LONG
                     | ?OTS_BOOLEAN
                     | ?OTS_DOUBLE
                     | ?OTS_STRING
                     | ?OTS_BINARY.

-type field_key()   :: {ots_key(), field_type()}.

-type field_value() :: atom()
                     | string()
                     | binary()
                     | integer()
                     | float()
                     | boolean().

-type field()       :: {field_key(), field_value()}.

-type fields()      :: [field()] | map().

-record(ts_row, {
    data_source :: ots_key(),
    tags = []   :: tags(),
    time        :: time(),
    fields = [] :: fields()
}).

-record(ts_rows, {
    measurement :: table(),
    rows        :: list(#ts_row{})
}).
