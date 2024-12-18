# The Erlang client for Tablestore

This is the Erlang client for [Tablestore](https://cn.aliyun.com/product/ots?from_alibabacloud=) based on the protobuf APIs.

Currently it only supports the **Timeseries** storage model.

## Build

```shell
$ rebar3 compile
```

## Examples

```erlang
{ok, _} = application:ensure_all_started(ots_erl),

Opts = [
    {instance, <<"my_instance">>},
    {pool, <<"my_pool_name">>},
    {endpoint, <<"https://my_instance.cn-hangzhou.ots.aliyuncs.com">>},
    {pool_size, 2},
    {access_key, <<"my_access_key_id">>},
    {access_secret, <<"my_access_key_secret">>}
],

{ok, C} = ots_ts_client:start(Opts),

Row = #{
    time => 1734517355998560,
    tags => #{
        <<"tagKey1">> => <<"tagValue1">>,
        <<"tagKey2">> => <<"tagValue2">>
    },
    fields => [
        {<<"bool_field">>, true, #{}},
        {<<"int_field">>, 10, #{isint => true}},
        {<<"double_field1">>, 10, #{}},
        {<<"double_field2">>, 1.3, #{}},
        {<<"str_field">>, <<"hello">>, #{}},
        {<<"bin_field">>, <<"hello">>, #{isbinary => true}}
    ],
    measurement => <<"measurement">>,
    data_source => <<"data_source">>
},

{ok, _Res} = ots_ts_client:put(C, #{
        table_name => <<"table_name">>,
        rows_data => [],
        meta_update_mode => 'MUM_NORMAL'
    }).
```

## Supported APIs for Timeseries model

- [x] ListTable
- [x] CreateTable
- [x] DeleteTable
- [x] DescribeTable
- [x] UpdateTable
- [x] GetData
- [x] PutData
- [x] QueryMeta
- [x] UpdateMeta
- [x] DeleteMeta
