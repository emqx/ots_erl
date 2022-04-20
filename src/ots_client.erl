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
        access_secret = proplists:get_value(access_secret, Opts)
    },
    do_start(Client, Opts).

stop(Client) ->
    Pool = pool(Client),
    hackney_pool:stop_pool(Pool).

list_ts_tables(Client) ->
    request(Client, ?FUNCTION_NAME, <<>>).

write(Client, Data) ->
    io:format("Encode ~p~n", [encode(Data)]),
    request(Client, ?FUNCTION_NAME, encode(Data)).

%% -------------------------------------------------------------------------------------------------
%% internal

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
            {ok, Client};
        Error ->
            Error
    end.

request(Client, Req, Data) ->
    ots_http:request(Client, endpoint(Req, Client), Data).

endpoint(Type, Client) ->
    case type(Client) of
        ?OTS_CLIENT_TS ->
            ts_endpoint(Type);
        ?OTS_CLIENT_WC ->
            %% TODO: wide column support
            {error, not_support}
    end.

ts_endpoint(list_ts_tables) -> ?LIST_TIMESERIES_TABLE;
ts_endpoint(write) -> ?PUT_TIMESERIES_DATA;
ts_endpoint(_) -> {error, not_support}.

%% -------------------------------------------------------------------------------------------------
%% app internal

pool(#ots_client{pool = P}) -> P.
type(#ots_client{type = T}) -> T.
endpoint(#ots_client{endpoint = E}) -> E.
instance(#ots_client{instance = I}) -> I.
access_key(#ots_client{access_key = K}) -> K.
access_secret(#ots_client{access_secret = S}) -> S.

%% -------------------------------------------------------------------------------------------------
%% ts data transform
encode(Rows) ->
    ots_sql:encode_msg(to_ots_data(Rows)).

to_ots_data(Rows) when is_list(Rows) ->
    #'TimeseriesRows'{
        type = 'RST_PROTO_BUFFER',
        rows_data = encode_pb_rows(Rows),
        flatbuffer_crc32c = undefined
    }.

encode_pb_rows(Rows) ->
    TimeseriesPBRows = #'TimeseriesPBRows'{
        rows = [to_row(Row) || Row <- Rows]
    },
    ots_sql:encode_msg(TimeseriesPBRows).

to_row(Row) ->
    Measurement = maps:get(measurement, Row),
    DataSource = maps:get(data_source, Row),
    Tags = maps:get(tags, Row, []),
    #'TimeseriesRow'{
        timeseries_key = to_timeseries_key(Measurement, DataSource, Tags),
        time = row_time(Row),
        fields = to_fields(maps:get(fields, Row, [])),
        %% TODO: meta_cache_update_time
        meta_cache_update_time = undefined
    }.

row_time(#{time := Time}) -> Time;
row_time(_Row) -> erlang:system_time(millisecond).

to_timeseries_key(Measurement, DataSource, Tags) ->
    #'TimeseriesKey'{
        measurement = Measurement,
        data_source = DataSource,
        tags = encode_tags(Tags)
    }.

encode_tags(Tags) when is_map(Tags) ->
    encode_tags(maps:to_list(Tags));
encode_tags(Tags) ->
    encode_tags(Tags, []).

encode_tags([], Res) ->
    list_to_binary(lists:reverse(Res));
encode_tags([{Key, Value}], Res) ->
    encode_tags([], [[to_binary(Key), <<"=">>, to_binary(Value)] | Res]);
encode_tags([{Key, Value} | Tags], Res) ->
    encode_tags(Tags, [[to_binary(Key), <<"=">>, to_binary(Value), <<",">>] | Res]).

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
