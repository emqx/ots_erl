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


-type ots_key()     :: atom()
                     | string()
                     | binary().

-define(OTS_LONG,    1).
-define(OTS_BOOLEAN, 2).
-define(OTS_DOUBLE,  3).
-define(OTS_STRING,  4).
-define(OTS_BINARY,  5).

%% some names: measurement, source ,table
-type table() :: ots_key().

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
-type time()        :: pos_integer().

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

-type fields()      :: [field()].

-record(ts_row, {
    tags = []   :: tags(),
    time        :: time(),
    fields = [] :: fields()
}).

-record(ts_rows, {
    measurement :: table(),
    rows        :: list(#ts_row{})
}).
