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

-module(ots_http).

-include("ots.hrl").
-include("ots_ep.hrl").

-export([request/3]).

request(Client, EndPoint, Body) ->
    Headers = headers(Client, EndPoint, Body),
    Pool = ots_client:pool(Client),
    Options = [
        {pool, Pool},
        {connect_timeout, 10000},
        {recv_timeout, 30000},
        {follow_redirectm, true},
        {max_redirect, 5},
        with_body
    ],
    Url = list_to_binary([ots_client:instance(Client), EndPoint]),
    case hackney:request(post, Url, Headers, Body, Options) of
        {ok, StatusCode, _Headers, ResponseBody}
            when StatusCode =:= 200
            orelse StatusCode =:= 204 ->
            {ok, ResponseBody};
        {ok, StatusCode, _Headers, ResponseBody} ->
            {error, {StatusCode, ResponseBody}};
        {error, Reason} ->
            {error, Reason}
    end.

headers(Client, EndPoint, Body) ->
    case ots_client:type(Client) of
        ?OTS_CLIENT_TS ->
            ts_headers(Client, EndPoint, Body);
        ?OTS_CLIENT_WC ->
            error({error, not_support})
    end.

ts_headers(Client, EndPoint, Body) ->
    %% Order by string name.
    %% If you don't know the rules of sorting, don't change it.
    InstanceName = ots_client:instance(Client),
    AccessKey = ots_client:access_key(Client),
    AccessSecret = ots_client:access_secret(Client),
    HeadersPart1 = [
        {<<"x-ots-accesskeyid">>, AccessKey},
        {<<"x-ots-apiversion">>, <<"2015-12-31">>},
        {<<"x-ots-contentmd5 ">>, base64:encode(erlang:md5(Body))},
        {<<"x-ots-date">>, iso8601:format(calendar:universal_time())},
        {<<"x-ots-instancename">>, InstanceName}
    ],
    Sign = {<<"x-ots-signature">>, sign(EndPoint, AccessSecret, HeadersPart1)},
    [Sign | HeadersPart1].

sign(EndPoint, AccessSecret, HeadersPart1) ->
    AccessSecret = <<"AccessKeyID">>,
    StringToSign = [
        EndPoint,
        "\nPOST\n\n"
    ],
    SignData = list_to_binary(
        [StringToSign | [[string:trim(H), ":", string:trim(V), "\n"] ||{H, V} <- HeadersPart1]]),
    crypto:mac(hmac, sha, AccessSecret, SignData).
