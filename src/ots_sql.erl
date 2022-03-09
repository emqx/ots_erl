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
    encode_ts_rows(TSRows).

%% -------------------------------------------------------------------------------------------------
%% rows

encode_ts_rows(TRows) ->
    [root_type_header(TRows) | encode_row_group(TRows)].

root_type_header(#ts_rows{rows = Rows}) ->
    RowsCount = erlang:length(Rows),
    RowsCountBinary = <<RowsCount:4/little-signed-integer-unit:8>>,
    Part1 = <<
    12, 0,  0,  0,
    6,  0,  8,  0,
    4,  0,  0,  0,
    6,  0,  0,  0,
    4,  0,  0,  0
    >>,
    %% only one FlatBufferRowGroup
    %% FlatBufferRowGroup list size 1, 0, 0, 0
    [<<Part1/binary, RowsCountBinary/binary>>, <<1, 0, 0, 0>>].

%% -------------------------------------------------------------------------------------------------
%% row group

encode_row_group(#ts_rows{measurement = M, rows = Rows}) ->
    % Vtable
    % 12, 0, 20,  0,
    % 4,  0,  8,  0,
    % 16, 0, 32,  0
    Part1 = <<
    20, 0, 0,  0,
    12, 0, 20, 0,
    4,  0, 8,  0,
    16, 0, 32, 0,
    16, 0, 0,  0
    >>,
    #{
        field_names_binary := FieldNamesBinary,
        field_types_binary := FieldTypesBinary,
        field_names := FieldsNames
    } = fields_info(hd(Rows)),
    MeasurementBinary = encode_string(M),
    MeasurementSize = erlang:size(MeasurementBinary),
    MeasurementOffset = 16,

    FieldNamesOffset = MeasurementSize + 12,
    FieldNamesSize = erlang:size(FieldNamesBinary),

    FieldTypesOffset = MeasurementSize + FieldNamesSize + 8,
    FieldTypesSize = erlang:size(FieldTypesBinary),

    RowsOffset = MeasurementSize + FieldNamesSize + FieldTypesSize + 4,

    Part2 = [
        Part1,
        <<MeasurementOffset:4/little-signed-integer-unit:8>>,
        <<FieldNamesOffset:4/little-signed-integer-unit:8>>,
        <<FieldTypesOffset:4/little-signed-integer-unit:8>>,
        MeasurementBinary,
        FieldNamesBinary,
        FieldTypesBinary
    ],
    [Part2, encode_ts_row(RowsOffset, FieldsNames, Rows)].

%% -------------------------------------------------------------------------------------------------
%% row in group
encode_ts_row(_RowsOffset, FieldsNames, Rows) ->
    % TODO: offset
    encode_ts_row_(FieldsNames, hd(Rows)).

encode_ts_row_(FieldsNames, #ts_row{data_source = DS, tags = Tags, time = Time, fields = Fields}) ->
    Part1 = <<
    20, 0, 0,  0,
    12, 0, 28, 0,
    4,  0, 8,  0,
    16, 0, 24, 0,
    28, 0, 0,  0,
    16, 0, 0,  0
    >>,
    DataSourceBinary = encode_string(DS),
    DataSourceSize = erlang:size(DataSourceBinary),
    DataSourceOffset = 24,

    TagsBinary = encode_ts_tags(Tags),
    TagsSize = erlang:size(TagsBinary),
    TagsOffset = DataSourceSize + 20,

    TimeBinary = encode_time(Time),

    FieldValuesBinary = encode_field_values(Fields, Fields),
    % TODO: use FieldValuesSize next row
    % FieldValuesSize = erlang:size(FieldValuesBinary),
    FieldValuesOffset = DataSourceSize + TagsSize + 4 + 8,

    %% TODO: client cache up time ?
    MetaCacheUpdateTimeBinary = <<0:32>>,

    Part2 = [
        Part1,
        <<DataSourceOffset:4/little-signed-integer-unit:8>>,
        <<TagsOffset:4/little-signed-integer-unit:8>>,
        TimeBinary,
        <<FieldValuesOffset:4/little-signed-integer-unit:8>>,
        MetaCacheUpdateTimeBinary,

        DataSourceBinary,
        TagsBinary,
        FieldValuesBinary
    ],
    [Part2, encode_field_values(FieldsNames, Fields)].

%% -------------------------------------------------------------------------------------------------
%% field values

encode_field_values(FieldsList, Fields) ->
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
        fun(Type, V, R) ->
            Values = encode_type_list(Type, lists:reverse(V)),
            maps:put(Type, Values, R)
        end,
    maps:fold(Fun, #{}, Res);
encode_field_value([Key = {_, Type} | FieldsList], Fields, Res) ->
    Value = get_value(Key, Fields),
    encode_field_value(FieldsList, Fields, Res#{Type => [Value | maps:get(Type, Res)]}).

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

get_value(Key, List) when is_list(List) ->
    proplists:get_value(Key, List);
get_value(Key, Map) when is_map(Map) ->
    maps:get(Key, Map).

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
%% time stamp

encode_time(undefined) ->
    Time = erlang:system_time(microsecond),
    <<Time:64>>;
encode_time(Time) when is_integer(Time) ->
    <<Time:64>>.

%% -------------------------------------------------------------------------------------------------
%% fields
fields_info(Row = #ts_row{fields = Fields}) when is_map(Fields) ->
    fields_info(Row#ts_row{fields = maps:to_list(Fields)});
fields_info(#ts_row{fields = Fields}) when is_list(Fields) ->
    #{
        field_names_binary => encode_field_names(Fields),
        field_types_binary => encode_field_types(Fields),
        field_names => [Name || {Name, _} <- Fields]
    }.

encode_field_names(Fields) ->
    Names = list_to_binary([encode_string(field_name(Field)) || Field <- Fields]),
    Size = erlang:length(Fields),
    SizeBinary = <<Size:4/little-signed-integer-unit:8>>,
    <<SizeBinary/binary, Names/binary>>.

encode_field_types(Fields) ->
    Types = padding(list_to_binary([field_type(Field) || Field <- Fields])),
    Size = erlang:length(Fields),
    SizeBinary = <<Size:4/little-signed-integer-unit:8>>,
    <<SizeBinary/binary, Types/binary>>.

field_name({{Key, _Type}, _}) -> to_binary(Key).

field_type({{Key, Type}, _Value}) -> Type.

%% -------------------------------------------------------------------------------------------------
%% long

encode_long_list(List) ->
    Len = erlang:length(List),
    Binary = list_to_binary([encode_long(Long) || Long <- List]),
    <<Len:4/little-signed-integer-unit:8, Binary/binary>>.

encode_long(Long) ->
    <<Long:4/little-signed-integer-unit:16>>.

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

encode_string_list(List) ->
    Len = erlang:length(List),
    StartOffset = Len * 4,
    StringListVector = encode_string_in_list(StartOffset, List, {[], []}),
    [<<Len:4/little-signed-integer-unit:8>>, StringListVector].

encode_string_in_list(_, [], {OffsetList, BinaryList}) ->
    [lists:reverse(OffsetList), lists:reverse(BinaryList)];
encode_string_in_list(StartOffset, [String | List], {OffsetList, BinaryList}) ->
    Binary = encode_string(String),
    Size = erlang:size(Binary),
    NextOffset = StartOffset + Size - 4,
    OffsetBinary = <<StartOffset:4/little-signed-integer-unit:8>>,
    encode_string_in_list(NextOffset, List,
        {[OffsetBinary | OffsetList], [Binary | BinaryList]}).

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

encode_binary_list(List) ->
    Len = erlang:length(List),
    StartOffset = Len * 4,
    StringListVector = encode_binary_in_list(StartOffset, List, {[], []}),
    [<<Len:4/little-signed-integer-unit:8>>, StringListVector].

encode_binary_in_list(_, [], {OffsetList, BinaryList}) ->
    [lists:reverse(OffsetList), lists:reverse(BinaryList)];
encode_binary_in_list(StartOffset, [Binary | List], {OffsetList, BinaryList}) ->
    BinaryEncode = encode_binary(Binary),
    Size = erlang:size(BinaryEncode),
    NextOffset = StartOffset + Size - 4,
    OffsetBinary = <<StartOffset:4/little-signed-integer-unit:8>>,
    encode_binary_in_list(NextOffset, List,
        {[OffsetBinary | OffsetList], [ BinaryEncode| BinaryList]}).

encode_binary(Binary) ->
    Size = erlang:size(Binary),
    Binary1 = padding(Binary),
    <<Size:4/little-signed-integer-unit:8, Binary1/binary>>.

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
