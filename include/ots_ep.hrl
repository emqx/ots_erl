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
