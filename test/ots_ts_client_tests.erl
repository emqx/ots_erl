%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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


-module(ots_ts_client_tests).

-include_lib("eunit/include/eunit.hrl").
-include("ots_ts_sql.hrl").

-define(OPTS(Pool), [
    {pool, Pool},
    {endpoint, <<"https://test.cn-hangzhou.ots.aliyuncs.com">>},
    {instance, <<"test-instance">>},
    {access_key, <<"test-access-key">>},
    {access_secret, <<"test-access-secret">>},
    {pool_size, 1}
]).

describe_table_test_() ->
    {setup,
        fun() -> start_client(ots_ts_client_test_pool) end,
        fun stop_client/1,
        fun(Client) ->
            [
                ?_test(describe_table_sends_table_name_payload(Client)),
                ?_test(describe_table_formats_success_response(Client)),
                ?_test(describe_table_formats_error_response(Client))
            ]
        end}.

list_tables_test_() ->
    {setup,
        fun() -> start_client(ots_ts_client_test_pool_list) end,
        fun stop_client/1,
        fun(Client) ->
            [
                ?_test(list_tables_formats_response(Client))
            ]
        end}.

start_client(Pool) ->
    {ok, _} = application:ensure_all_started(hackney),
    {ok, _} = application:ensure_all_started(ots_erl),
    meck:new(hackney, [no_history]),
    {ok, Client} = ots_ts_client:start(?OPTS(Pool)),
    Client.

stop_client(Client) ->
    ots_ts_client:stop(Client),
    meck:unload(hackney).

describe_table_sends_table_name_payload(Client) ->
    meck:expect(hackney, request, fun(post, _Url, _Headers, Payload, _Opts) ->
        put(ots_payload, Payload),
        {ok, 200, [{<<"x-ots-requestid">>, <<"req-1">>}], describe_table_ok_body()}
    end),
    {ok, _} = ots_ts_client:describe_table(Client, #{table_name => <<"probe_table">>}),
    Payload = get(ots_payload),
    ?assertNotEqual(<<>>, Payload),
    ?assertEqual(
        #'DescribeTimeseriesTableRequest'{table_name = "probe_table"},
        ots_ts_sql:decode_msg(Payload, 'DescribeTimeseriesTableRequest')
    ).

describe_table_formats_success_response(Client) ->
    meck:expect(hackney, request, fun(post, _Url, _Headers, _Payload, _Opts) ->
        {ok, 200, [{<<"x-ots-requestid">>, <<"req-1">>}], describe_table_ok_body()}
    end),
    ?assertMatch(
        {ok, #{table_name := "probe_table", status := "ACTIVE", time_to_live := 3}},
        ots_ts_client:describe_table(Client, #{table_name => <<"probe_table">>})
    ).

describe_table_formats_error_response(Client) ->
    meck:expect(hackney, request, fun(post, _Url, _Headers, _Payload, _Opts) ->
        {ok, 400, [{<<"x-ots-requestid">>, <<"req-2">>}], describe_table_error_body()}
    end),
    ?assertMatch(
        {error, #{code := "OTSParameterInvalid", message := "bad table",
                  http_code := 400, request_id := <<"req-2">>}},
        ots_ts_client:describe_table(Client, #{table_name => <<"probe_table">>})
    ).

list_tables_formats_response(Client) ->
    meck:expect(hackney, request, fun(post, _Url, _Headers, _Payload, _Opts) ->
        {ok, 200, [{<<"x-ots-requestid">>, <<"req-3">>}], list_tables_ok_body()}
    end),
    ?assertMatch(
        {ok, [#{table_name := "table_a", status := "ACTIVE", time_to_live := 3}]},
        ots_ts_client:list_tables(Client)
    ).

describe_table_ok_body() ->
    ots_ts_sql:encode_msg(#'DescribeTimeseriesTableResponse'{
        table_meta = #'TimeseriesTableMeta'{
            table_name = <<"probe_table">>,
            table_options = #'TimeseriesTableOptions'{time_to_live = 3},
            status = <<"ACTIVE">>
        }
    }).

describe_table_error_body() ->
    ots_ts_sql:encode_msg(#'ErrorResponse'{
        code = <<"OTSParameterInvalid">>,
        message = <<"bad table">>
    }).

list_tables_ok_body() ->
    ots_ts_sql:encode_msg(#'ListTimeseriesTableResponse'{
        table_metas = [
            #'TimeseriesTableMeta'{
                table_name = <<"table_a">>,
                table_options = #'TimeseriesTableOptions'{time_to_live = 3},
                status = <<"ACTIVE">>
            }
        ]
    }).
