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
-module(ots_sql).
-include("ots.hrl").

-export([ ts_row/2
        , ts_row/3
        , ts_rows/2
        ]).

-export([ encode/1]).

-export([encode_ts_tags/1]).

ts_row(Tags, Fields) ->
    ts_row(Tags, erlang:system_time(microsecond), Fields).

ts_row(Tags, Time, Fields) ->
    #ts_row{
        tags = Tags,
        time = Time,
        fields = Fields
    }.

ts_rows(Measurement, Rows) ->
    #ts_rows{
        measurement = Measurement,
        rows = Rows
    }.

encode(TSRows) when is_record(TSRows, ts_rows) ->
    ok.

%% -------------------------------------------------------------------------------------------------
%% rows

encode_ts_rows(#ts_rows{measurement = Measurement, rows = Rows}) ->
    FieldsList = fields(hd(Rows)),
    RowsBinary = [encode_ts_row(FieldsList, Row) || Row <- Rows],
    %% TODO: table & vtable
    ok.
%% -------------------------------------------------------------------------------------------------
%% row

encode_ts_row(FieldsList, #ts_row{tags = Tags, time = Time, fields = Fields}) ->
    TagsBinary = encode_ts_tags(Tags),
    FieldsBinary = encode_fields(FieldsList, Fields),
    ok.

%% -------------------------------------------------------------------------------------------------
%% tags

encode_ts_tags([]) ->
    <<"[]">>;
encode_ts_tags([Tag1]) ->
    Tag = encode_ts_tag(Tag1, true),
    <<"[",  Tag/binary, "]">>;
encode_ts_tags([Tag1 | Tags]) ->
    First = [<<"[">>, encode_ts_tag(Tag1, true)],
    TagsBinary = [encode_ts_tag(Tag) || Tag <- Tags],
    list_to_binary(lists:append([First | TagsBinary], [<<"]">>])).

encode_ts_tag(Tag) ->
    encode_ts_tag(Tag, false).
encode_ts_tag({K, V}, First) ->
    Key = to_binary(K),
    Value = to_binary(V),
    case First of
        true ->
            <<"\"", Key/binary, "=", Value/binary, "\"">>;
        false ->
            <<",\"", Key/binary, "=", Value/binary, "\"">>
    end.

%% -------------------------------------------------------------------------------------------------
%% fields
fields(Row = #ts_row{fields = Fields}) when is_map(Fields) ->
    fields(Row#ts_row{fields = maps:to_list(Fields)});
fields(#ts_row{fields = Fields}) when is_list(Fields) ->
    [field_type(Field) || Field <- Fields].

field_type({{Key, Type}, _Value}) -> {Key, Type};
% Type derivation
field_type({Key, Value}) when is_integer(Value) -> {Key, ?OTS_LONG};
field_type({Key, Value}) when is_boolean(Value) -> {Key, ?OTS_BOOLEAN};
field_type({Key, Value}) when is_float(Value)   -> {Key, ?OTS_DOUBLE};
field_type({Key, Value}) when is_list(Value)    -> {Key, ?OTS_STRING};
field_type({Key, Value}) when is_binary(Value)  -> {Key, ?OTS_BINARY};
field_type({Key, Value}) when is_atom(Value)    -> {Key, ?OTS_STRING}.

encode_fields(FieldsList, Fields) ->
    % #{
    %    ?OTS_LONG    => long_values,
    %    ?OTS_BOOLEAN => bool_values,
    %    ?OTS_DOUBLE  => double_values,
    %    ?OTS_STRING  => string_values,
    %    ?OTS_BINARY  => binary_values
    % },
    Init = #{
        ?OTS_LONG    => [],
        ?OTS_BOOLEAN => [],
        ?OTS_DOUBLE  => [],
        ?OTS_STRING  => [],
        ?OTS_BINARY  => []
     },
    encode_field_value(FieldsList, Fields, Init).

encode_field_value([], _Fields, Res) ->
    Fun =
        fun(K, V, R) ->
            Values = encode_type_list(K, lists:reverse(V)),
            maps:put(K, Values, R)
        end,
    maps:fold(Fun, #{}, Res);
encode_field_value([Key = {_, Type} | FieldsList], Fields, Res) ->
    Value = get_value(Key, Fields),
    encode_field_value(FieldsList, Fields, Res#{Type => [Value | maps:get(Type, Res)]}).

get_value(Key, List) when is_list(List) ->
    proplists:get_value(Key, List);
get_value(Key, Map) when is_map(Map) ->
    maps:get(Key, Map).

encode_fields_keys(Fields) ->
    [encode_string(Key) || Key <- proplists:get_keys(Fields)].

encode_fields_types(Fields) ->
    Types = padding(list_to_binary([field_type(Field) || Field <- Fields])),
    Size = erlang:length(Fields),
    <<Size:4/little-signed-integer-unit:8, Types/binary>>.

%% -------------------------------------------------------------------------------------------------
%% util
to_binary(X) when is_binary(X)  -> X;
to_binary(X) when is_list(X)    -> list_to_binary(X);
to_binary(X) when is_integer(X) -> integer_to_binary(X);
to_binary(X) when is_float(X)   -> float_to_binary(X, [{decimals, 10}, compact]);
to_binary(X) when is_atom(X)    -> atom_to_binary(X, utf8).

padding(Bin) when is_binary(Bin) ->
    Size = erlang:size(Bin),
    Padding = (4 - (Size rem 4)) * 8,
    <<Bin/binary, 0:Padding>>.

encode_type_list(Type, List) ->
    case Type of
        ?OTS_LONG ->
            encode_long_list(List);
        ?OTS_BOOLEAN ->
            encode_boolean_list(List);
        ?OTS_DOUBLE ->
            encode_double_list(List);
        ?OTS_STRING ->
            encode_string_list(List);
        ?OTS_BINARY ->
            encode_binary_list(List)
    end.
%% -------------------------------------------------------------------------------------------------
%% long

encode_long_list(List) ->
    Len = erlang:length(List),
    Binary = list_to_binary([encode_long(Long) || Long <- List]),
    <<Len:4/little-signed-integer-unit:8, Binary/binary>>.

encode_long(Long) ->
    <<Long:4/little-signed-integer-unit:8>>.

%% -------------------------------------------------------------------------------------------------
%% boolean

encode_boolean_list(List) ->
    Len = erlang:length(List),
    Binary = padding(list_to_binary([encode_boolean(Boolean) || Boolean <- List])),
    <<Len:4/little-signed-integer-unit:8, Binary/binary>>.

encode_boolean(true) -> 1;
encode_boolean(false) -> 0.

%% -------------------------------------------------------------------------------------------------
%% double

encode_double_list(List) ->
    Len = erlang:length(List),
    Binary = list_to_binary([encode_double(Double) || Double <- List]),
    <<Len:4/little-signed-integer-unit:8, Binary/binary>>.

encode_double(Data) ->
    <<Data:64/little-signed-float>>.

%% -------------------------------------------------------------------------------------------------
%% string

encode_string_list(_List) ->
    %% TODO: VTable
    ok.

encode_string(Data) ->
    Binary = to_binary(Data),
    Size = erlang:size(Binary),
    Padding = string_padding(Binary),
    <<Size:4/little-signed-integer-unit:8, Padding/binary>>.

string_padding(Bin) ->
    case padding(Bin) of
        Bin ->
            Padding = 4 * 8,
            <<Bin/binary, 0:Padding>>;
        Res ->
            Res
    end.


%% -------------------------------------------------------------------------------------------------
%% binary
encode_binary_list(_List) ->
    %% TODO: VTable
    ok.
