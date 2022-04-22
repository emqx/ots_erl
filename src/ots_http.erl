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

-export([request/3]).

request(Client, Action, Body) ->
    Headers = headers(Client, Action, Body),
    Pool = ots_client:pool(Client),
    Options = [
        {pool, Pool},
        {connect_timeout, 10000},
        {recv_timeout, 30000},
        {follow_redirectm, true},
        {max_redirect, 5},
        with_body
    ],
    Url = list_to_binary([ots_client:endpoint(Client), Action]),
    case hackney:request(post, Url, Headers, Body, Options) of
        {ok, StatusCode, _Headers, ResponseBody}
            when StatusCode =:= 200
            orelse StatusCode =:= 204 ->
            {ok, ResponseBody};
        {ok, StatusCode, RespHeaders, ResponseBody} ->
            Reason = #{
                code => StatusCode,
                request_id => proplists:get_value(<<"x-ots-requestid">>, RespHeaders),
                message => ResponseBody
            },
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

headers(Client, Action, Body) ->
    case ots_client:type(Client) of
        ?OTS_CLIENT_TS ->
            ts_headers(Client, Action, Body);
        ?OTS_CLIENT_WC ->
            error({error, not_support})
    end.

ts_headers(Client, Action, Body) ->
    %% Order by string name.
    %% [{header_a, _}, {header_b, _}, ....]
    %% If you don't know the rules of sorting, don't change it.
    HeadersPart1 = [
        {<<"x-ots-accesskeyid">>  , ots_client:access_key(Client)},
        {<<"x-ots-apiversion">>   , <<"2015-12-31">>},
        {<<"x-ots-contentmd5">>   , base64:encode(erlang:md5(Body))},
        {<<"x-ots-date">>         , iso8601_now()},
        {<<"x-ots-instancename">> , ots_client:instance(Client)}
    ],
    Sign = {<<"x-ots-signature">>, sign(Action, ots_client:access_secret(Client), HeadersPart1)},
    lists:append(HeadersPart1, [Sign]).

iso8601_now() ->
    {DatePart1, {H, M, S}} = calendar:universal_time(),
    iso8601:format({DatePart1, {H, M, S * 1.0}}).

sign(Action, AccessSecret, HeadersPart1) ->
    StringToSign = [
        Action,
        "\nPOST\n\n"
    ],
    SignData = list_to_binary(
        [StringToSign | [[string:trim(H), ":", string:trim(V), "\n"] ||{H, V} <- HeadersPart1]]),
    base64:encode(crypto:mac(hmac, sha, AccessSecret, SignData)).
