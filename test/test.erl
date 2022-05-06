-module(test).

-export([test/0]).

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

    Tags1 = [
        {"t_int", 1},
        {<<"t_double">>, 1.1},
        {t_boolean, true},
        {"t_string", "string1"},
        {t_binary1, <<"binary1">>},
        {t_a_string, atom1}
    ],

    Tags2 = #{
        "t_int" => 2,
        <<"t_double">> => 1.1,
        t_binary1 => <<"binary1">>,
        t_boolean => true,
        t_string => "string1",
        t_a_string => atom1
    },

    Tags3 = [{"t_int", 3}],
    Tags4 = #{},

    Rows = [
        % #{measurement => <<"measurement1">>, data_source => <<"data_source1">>, fields => Fields1, tags => Tags1}
        % #{meta_update_mode => 'MUM_NORMAL', measurement => <<"measurement1">>, data_source => <<"data_source1">>, fields => Fields1, tags => Tags2},
        #{measurement => <<"measurement1">>, data_source => <<"data_source1">>, fields => Fields2, tags => Tags4}
    ],
    Data = #{
        table_name => <<"flatbuffer_tab_test">>,
        rows_data => Rows,
        meta_update_mode => 'MUM_NORMAL'
    },
    %% remove this fun after released
    % ots_client:test().
    Opts = [
        {pool, test_demo_pool},
        % {endpoint, <<"https://emqx-demo.cn-hangzhou.ots.aliyuncs.com">>},
        {endpoint, <<"https://emqx-test.cn-hangzhou.ots.aliyuncs.com">>},
        {instance, <<"emqx-test">>},
        {access_key, <<"LTAI5tETEEvA4D7ctpSYvmEg">>},
        {access_secret, <<"">>},
        {pool_size, 1}
    ],
    {ok, Client} = ots:start(Opts),
    % List = ots_client:list_ts_tables(Client),
    % IsAlive = ots:is_alive(Client),
    % Write = ots:write(Client, Data),
    % read_response("ListTable", List),
    % read_response("IsAlive", IsAlive),
    % read_response("Write", Write),
    timer:sleep(2000),
    % Write2 = ots:write(Client, Data),
    % read_response("Write2", Write2),
    %% ets:tab2list(ots_cache_test_demo_pool).
    Stop = ots:stop(Client),
    read_response("Stop", Stop),
    ok.


read_response(Title, {error, {Code, Message}}) ->
    io:format("~p ~p: ~s~n", [Title, Code, Message]);

read_response(Title, {ok, Message}) ->
    io:format("~p : ~0p~n", [Title, Message]);

read_response(Title, Message) ->
    io:format("~p : ~p~n", [Title, Message]).
