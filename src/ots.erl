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

-module(ots).

-include("ots.hrl").

-export([ start/1
        , is_alive/1
        , write/2
        , stop/1
        ]).

-export([test/0]).

start(Opts) ->
    ots_client:start(Opts).

stop(Client) ->
    ots_client:stop(Client).

is_alive(Client) ->
    case ots_client:list_ts_tables(Client) of
        {error, _} ->
            false;
        {ok, _} ->
            true
    end.

write(Client, Data) ->
    ots_client:write(Client, Data).

test() ->
    Fields1 = [
        {"f_int", 1},
        {<<"f_double">>, 1.1},
        {f_boolean, true},
        {f_string, "string1"},
        {f_binary1, <<"binary1">>},
        {f_a_string, atom1}
    ],

    Fields2 = #{
        "f_int" => 2,
        <<"f_binary1">> => <<"binary2">>,
        f_boolean => false,
        f_double => 1.2,
        f_string => "string2",
        f_a_string => atom2
    },

    Fields3 = [],
    Fields4 = #{},

    Tags1 = [
        {"t_int", 1},
        {<<"t_double">>, 1.1},
        {t_boolean, true},
        {t_string, "string1"},
        {t_binary1, <<"binary1">>},
        {t_a_string, atom1}
    ],

    Tags2 = #{
        "t_int" => 1,
        <<"t_double">> => 1.1,
        t_binary1 => <<"binary1">>,
        t_boolean => true,
        t_string => "string1",
        t_a_string => atom1
    },

    Tags3 = [],
    Tags4 = #{},

    Rows = [
        #{measurement => <<"measurement1">>, data_source => <<"data_source1">>, fields => Fields1, tags => Tags1}
        % #{measurement => <<"measurement1">>, data_source => <<"data_source1">>, fields => Fields1, time => 10000000002},
        % #{measurement => <<"measurement2">>, data_source => <<"data_source2">>, fields => Fields2, tags => Tags2},
        % #{measurement => <<"measurement3">>, data_source => <<"data_source3">>, fields => Fields3, time => 10000000003, tags => Tags3},
        % #{measurement => <<"measurement4">>, data_source => <<"data_source4">>, fields => Fields4, tags => Tags4},
        % #{measurement => <<"measurement4_no_tag">>, data_source => <<"data_source4">>, fields => Fields4}
    ],
    %% remove this fun after released
    % ots_client:test().
    Opts = [
        {pool, test_demo_pool},
        % {endpoint, <<"https://emqx-demo.cn-hangzhou.ots.aliyuncs.com">>},
        {endpoint, <<"https://emqx-test.cn-hangzhou.ots.aliyuncs.com">>},
        {instance, <<"emqx-test">>},
        {access_key, <<"LTAI5tETEEvA4D7ctpSYvmEg">>},
        {access_secret, <<"6rEq8kuTlLAaTPlEG86V7ip6YjELBQ">>},
        {pool_size, 1}
    ],
    {ok, Client} = start(Opts),
    List = ots_client:list_ts_tables(Client),
    IsAlive = is_alive(Client),
    Write = write(Client, Rows),
    read_response("ListTable", List),
    read_response("IsAlive", IsAlive),
    read_response("Write", Write),
    ok.

read_response(Title, {error, {Code, Message}}) ->
    io:format("~p ~p: ~s~n", [Title, Code, Message]);

read_response(Title, {ok, Message}) ->
    io:format("~p : ~s~n", [Title, Message]);

read_response(Title, Message) ->
    io:format("~p : ~p~n", [Title, Message]).

