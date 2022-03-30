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
        , stop/1
        ]).

-export([ pool/1
        , type/1
        , endpoint/1
        , instance/1
        , access_key/1
        , access_secret/1
        ]).

-export([test/0]).

test() ->
    %% remove this fun after released
    % ots_client:test().
    Opts = [
        {pool, test_demo_pool},
        {endpoint, <<"https://emqx-demo.cn-hangzhou.ots.aliyuncs.com">>},
        {instance, <<"emqx-demo">>},
        {access_key, <<"LTAI5tETEEvA4D7ctpSYvmEg">>},
        {access_secret, <<"not pwd in github">>},
        {pool_size, 1}
    ],
    {ok, Client} = start(Opts),
    case list_ts_tables(Client) of
        {error, {Code, Resp}} ->
            io:format("Res ~p ~s ~n", [Code, to_str(Resp)]);
        {ok, R} ->
            io:format("~s~n", [to_str(R)])
    end,
    ok.

to_str(B) when is_binary(B) -> binary_to_list(B);
to_str(B) -> B.

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
    start_client(Client, Opts).

stop(Client) ->
    Pool = pool(Client),
    hackney_pool:stop_pool(Pool).

list_ts_tables(Client) ->
    ots_http:request(Client, ?LIST_TIMESERIES_TABLE, <<>>).

%% -------------------------------------------------------------------------------------------------
%% internal

start_client(Client, Opts) ->
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

%% -------------------------------------------------------------------------------------------------
%% app internal

pool(#ots_client{pool = P}) -> P.
type(#ots_client{type = T}) -> T.
endpoint(#ots_client{endpoint = E}) -> E.
instance(#ots_client{instance = I}) -> I.
access_key(#ots_client{access_key = K}) -> K.
access_secret(#ots_client{access_secret = S}) -> S.
