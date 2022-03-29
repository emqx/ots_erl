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
% -inclur tde("ots.hrl").

% -export([ ts_row/3
%         , ts_row/4
%         , ts_rows/2
%         ]).

% -export([ encode/1]).

% -export([test/0
%         , encode_ts_tags/1
%         ]).

% ts_row(DataSource, Tags, Fields) ->
%     ts_row(DataSource, Tags, erlang:system_time(microsecond), Fields).

% ts_row(DataSource, Tags, Time, Fields) ->
%     #ts_row{
%         data_source = DataSource,
%         tags = Tags,
%         time = Time,
%         fields = Fields
%     }.

% ts_rows(Measurement, Rows) ->
%     #ts_rows{
%         measurement = Measurement,
%         rows = Rows
%     }.

% test() ->
%     DataSource1 = <<"d_source1">>,
%     DataSource2 = <<"d_source2">>,
%     Tags = [{tag1, t1}, {tag1, t2}],
%     Fields = [
%         {{field_s, ?OTS_STRING}, f1},
%         {{field_s2, ?OTS_STRING}, f2},
%         {{field_i, ?OTS_LONG}, 123},
%         {{field_i2, ?OTS_LONG}, 234},
%         {{field_d, ?OTS_DOUBLE}, 1.1},
%         {{field_d2, ?OTS_DOUBLE}, 1.2},
%         {{field_b, ?OTS_BOOLEAN}, false},
%         {{field_b2, ?OTS_BOOLEAN}, true},
%         {{field_bi, ?OTS_BINARY}, <<"aaaa">>},
%         {{field_bi2, ?OTS_BINARY}, <<"abcdef2">>}
%     ],
%     Time = erlang:system_time(microsecond),
%     Measurement = <<"measurement">>,

%     Row1 = ts_row(DataSource1, Tags, Time, Fields),
%     % Row2 = ts_row(DataSource2, Tags, Fields),

%     Rows = ts_rows(Measurement, [
%         Row1
%         % , Row2
%     ]),
%     io:format("Rows ~p~n", [Rows]),
%     Encode = list_to_binary(encode(Rows)),
%     io:format("Encode ~p~n", [Encode]),
%     io:format("Encode size ~p ~p~n", [size(Encode), size(Encode) rem 4]),
%     ok.

% encode(TSRows) when is_record(TSRows, ts_rows) ->
%     encode_ts_rows(TSRows).

% %% -------------------------------------------------------------------------------------------------
% %% rows

% encode_ts_rows(TRows) ->
%     Part1 = <<
%     12, 0,  0,  0,
%     6,  0,  8,  0,
%     4,  0,  0,  0,
%     8,  0,  0,  0,
%     4,  0,  0,  0,
%     1,  0,  0,  0
%     >>,
%     [Part1, encode_row_group(TRows)].

% %% -------------------------------------------------------------------------------------------------
% %% row group

% encode_row_group(#ts_rows{measurement = M, rows = Rows}) ->
%     % Vtable
%     % 12, 0, 20,  0,
%     % 4,  0,  8,  0,
%     % 16, 0, 32,  0
%     Part1 = <<
%     20, 0, 0,  0,
%     12, 0, 20, 0,
%     4,  0, 8,  0,
%     16, 0, 32, 0,
%     16, 0, 0,  0
%     >>,
%     #{
%         field_names_binary := FieldNamesBinary,
%         field_types_binary := FieldTypesBinary,
%         field_names := FieldsNames
%     } = fields_info(hd(Rows)),
%     MeasurementBinary = encode_string(M),
%     MeasurementSize = erlang:size(MeasurementBinary),
%     MeasurementOffset = 16,

%     FieldNamesOffset = MeasurementSize + 12,
%     FieldNamesSize = erlang:size(FieldNamesBinary),

%     FieldTypesOffset = MeasurementSize + FieldNamesSize + 8,
%     FieldTypesSize = erlang:size(FieldTypesBinary),

%     RowsOffset = MeasurementSize + FieldNamesSize + FieldTypesSize + 4,

%     Part2 = [
%         Part1,
%         lsiu(MeasurementOffset),
%         lsiu(FieldNamesOffset),
%         lsiu(FieldTypesOffset),
%         lsiu(RowsOffset),
%         MeasurementBinary,
%         FieldNamesBinary,
%         FieldTypesBinary
%     ],
%     [Part2, encode_ts_rows(FieldsNames, Rows)].

% %% -------------------------------------------------------------------------------------------------
% %% row in group

% encode_ts_rows(FieldsNames, Rows) ->
%     Len = erlang:length(Rows),
%     StartOffset = Len * 4,
%     [lsiu(StartOffset), encode_ts_rows(StartOffset, FieldsNames, Rows, {[], []})].

% encode_ts_rows(_, _, [], {OffsetList, BinaryList}) ->
%     [lists:reverse(OffsetList), lists:reverse(BinaryList)];
% encode_ts_rows(StartOffset, FieldsNames, [Row | Rows], {OffsetList, BinaryList}) ->
%     RowBinary = encode_ts_row(FieldsNames, Row),
%     RowSize = erlang:size(RowBinary),
%     NextOffset = StartOffset + RowSize - 4,
%     OffsetBinary = lsiu(StartOffset),
%     encode_ts_rows(NextOffset, FieldsNames, Rows, {
%         [OffsetBinary, OffsetList], [RowBinary | BinaryList]
%     }).

% encode_ts_row(FieldsNames, #ts_row{data_source = DS, tags = Tags, time = Time, fields = Fields}) ->
%     Part1 = <<
%     20, 0, 0,  0,
%     12, 0, 28, 0,
%     4,  0, 8,  0,
%     16, 0, 24, 0,
%     28, 0, 0,  0,
%     16, 0, 0,  0
%     >>,
%     DataSourceBinary = encode_string(DS),
%     DataSourceSize = erlang:size(DataSourceBinary),
%     DataSourceOffset = 24,

%     TagsBinary = encode_ts_tags(Tags),
%     TagsSize = erlang:size(TagsBinary),
%     TagsOffset = DataSourceSize + 20,

%     TimeBinary = encode_time(Time),

%     io:format("~p : ~p~n ~p~n", [?FUNCTION_NAME, FieldsNames, Fields]),
%     FieldValuesBinary = encode_field_values(FieldsNames, Fields),
%     FieldValuesOffset = DataSourceSize + TagsSize + 4 + 8,

%     %% TODO: client cache up time ?
%     MetaCacheUpdateTimeBinary = <<0:32>>,

%     list_to_binary([
%         Part1,
%         lsiu(DataSourceOffset),
%         lsiu(TagsOffset),
%         TimeBinary,
%         lsiu(FieldValuesOffset),
%         MetaCacheUpdateTimeBinary,

%         DataSourceBinary,
%         TagsBinary,
%         FieldValuesBinary
%     ]).

% %% -------------------------------------------------------------------------------------------------
% %% field values

% encode_field_values(FieldsList, Fields) ->
%     % #{
%     %    ?OTS_LONG    => long_values,
%     %    ?OTS_BOOLEAN => bool_values,
%     %    ?OTS_DOUBLE  => double_values,
%     %    ?OTS_STRING  => string_values,
%     %    ?OTS_BINARY  => binary_values
%     % },
%     Part1 = <<
%     24,  0,  0,  0,
%     14,  0, 24,  0,
%     4,   0,  8,  0,
%     16,  0, 20,  0,
%     24,  0,  0,  0,
%     20,  0,  0,  0
%     >>,
%     Init = #{
%         ?OTS_LONG    => [],
%         ?OTS_BOOLEAN => [],
%         ?OTS_DOUBLE  => [],
%         ?OTS_STRING  => [],
%         ?OTS_BINARY  => []
%      },
%     ValuesMap = encode_field_value(FieldsList, Fields, Init),
%     %% length([?OTS_LONG,?OTS_BOOLEAN,?OTS_DOUBLE ,?OTS_STRING ,?OTS_BINARY ]) = 5
%     %% StartOffset: 5 * 4 + 4 = 24
%     StartOffset = 24,
%     TypeList = [?OTS_LONG, ?OTS_BOOLEAN, ?OTS_DOUBLE, ?OTS_STRING, ?OTS_BINARY],
%     Part2 = encode_field_values(StartOffset, TypeList, ValuesMap, {[], []}),
%     [Part1, Part2].

% encode_field_values(_, [], _, {OffsetList, BinaryList}) ->
%     [
%         lists:reverse(OffsetList),
%         lists:reverse(BinaryList)
%     ];
% encode_field_values(StartOffset, [Type | Types], ValuesMap, {OffsetList, BinaryList}) ->
%     Binary = maps:get(Type, ValuesMap),
%     Size = erlang:size(Binary),
%     NextOffset = StartOffset + Size - 4,
%     encode_field_values(NextOffset, Types, ValuesMap,
%         {[lsiu(StartOffset) | OffsetList], [Binary | BinaryList]}).

% encode_field_value([], _Fields, Res) ->
%     Fun =
%         fun(Type, V, R) ->
%             Values = encode_type_list(Type, lists:reverse(V)),
%             maps:put(Type, Values, R)
%         end,
%     maps:fold(Fun, #{}, Res);
% encode_field_value([Key = {_, Type} | FieldsList], Fields, Res) ->
%     Value = get_value(Key, Fields),
%     encode_field_value(FieldsList, Fields, Res#{Type => [Value | maps:get(Type, Res)]}).

% encode_type_list(Type, List) ->
%     case Type of
%         ?OTS_LONG ->
%             encode_long_list(List);
%         ?OTS_BOOLEAN ->
%             encode_boolean_list(List);
%         ?OTS_DOUBLE ->
%             encode_double_list(List);
%         ?OTS_STRING ->
%             encode_string_list(List);
%         ?OTS_BINARY ->
%             encode_binary_list(List)
%     end.

% get_value(Key, List) when is_list(List) ->
%     proplists:get_value(Key, List);
% get_value(Key, Map) when is_map(Map) ->
%     maps:get(Key, Map).

% %% -------------------------------------------------------------------------------------------------
% %% tags

% encode_ts_tags([]) ->
%     encode_string(<<"[]">>);
% encode_ts_tags([Tag1]) ->
%     Tag = encode_ts_tag(Tag1, true),
%     encode_string(<<"[",  Tag/binary, "]">>);
% encode_ts_tags([Tag1 | Tags]) ->
%     First = [<<"[">>, encode_ts_tag(Tag1, true)],
%     TagsBinary = [encode_ts_tag(Tag) || Tag <- Tags],
%     encode_string(lists:append([First | TagsBinary], [<<"]">>])).

% encode_ts_tag(Tag) ->
%     encode_ts_tag(Tag, false).
% encode_ts_tag({K, V}, First) ->
%     Key = to_binary(K),
%     Value = to_binary(V),
%     case First of
%         true ->
%             <<"\"", Key/binary, "=", Value/binary, "\"">>;
%         false ->
%             <<",\"", Key/binary, "=", Value/binary, "\"">>
%     end.

% %% -------------------------------------------------------------------------------------------------
% %% time stamp

% encode_time(undefined) ->
%     Time = erlang:system_time(microsecond),
%     <<Time:64>>;
% encode_time(Time) when is_integer(Time) ->
%     <<Time:64>>.

% %% -------------------------------------------------------------------------------------------------
% %% fields
% fields_info(Row = #ts_row{fields = Fields}) when is_map(Fields) ->
%     fields_info(Row#ts_row{fields = maps:to_list(Fields)});
% fields_info(#ts_row{fields = Fields}) when is_list(Fields) ->
%     #{
%         field_names_binary => encode_field_names(Fields),
%         field_types_binary => encode_field_types(Fields),
%         field_names => [Name || {Name, _} <- Fields]
%     }.

% encode_field_names(Fields) ->
%     encode_string_list([field_name(Field) || Field <- Fields]).

% encode_field_types(Fields) ->
%     Types = padding(list_to_binary([field_type(Field) || Field <- Fields])),
%     Size = erlang:length(Fields),
%     SizeBinary = lsiu(Size),
%     <<SizeBinary/binary, Types/binary>>.

% field_name({{Key, _Type}, _}) -> to_binary(Key).

% field_type({{_Key, Type}, _}) -> Type.

% %% -------------------------------------------------------------------------------------------------
% %% long

% encode_long_list(List) ->
%     Len = erlang:length(List),
%     Binary = list_to_binary([encode_long(Long) || Long <- List]),
%     <<Len:4/little-signed-integer-unit:8, Binary/binary>>.

% encode_long(Long) ->
%     <<Long:4/little-signed-integer-unit:16>>.

% %% -------------------------------------------------------------------------------------------------
% %% boolean

% encode_boolean_list(List) ->
%     Len = erlang:length(List),
%     Binary = padding(list_to_binary([encode_boolean(Boolean) || Boolean <- List])),
%     <<Len:4/little-signed-integer-unit:8, Binary/binary>>.

% encode_boolean(true) -> 1;
% encode_boolean(false) -> 0.

% %% -------------------------------------------------------------------------------------------------
% %% double

% encode_double_list(List) ->
%     Len = erlang:length(List),
%     Binary = list_to_binary([encode_double(Double) || Double <- List]),
%     <<Len:4/little-signed-integer-unit:8, Binary/binary>>.

% encode_double(Data) ->
%     <<Data:64/little-signed-float>>.

% %% -------------------------------------------------------------------------------------------------
% %% string

% encode_string_list(List) ->
%     Len = erlang:length(List),
%     StartOffset = Len * 4,
%     StringListVector = encode_string_in_list(StartOffset, List, {[], []}),
%     list_to_binary([lsiu(Len), StringListVector]).

% encode_string_in_list(_, [], {OffsetList, BinaryList}) ->
%     [lists:reverse(OffsetList), lists:reverse(BinaryList)];
% encode_string_in_list(StartOffset, [String | List], {OffsetList, BinaryList}) ->
%     Binary = encode_string(String),
%     Size = erlang:size(Binary),
%     NextOffset = StartOffset + Size - 4,
%     OffsetBinary = lsiu(StartOffset),
%     encode_string_in_list(NextOffset, List,
%         {[OffsetBinary | OffsetList], [Binary | BinaryList]}).

% encode_string(Data) ->
%     Binary = to_binary(Data),
%     Size = erlang:size(Binary),
%     Padding = string_padding(Binary),
%     <<Size:4/little-signed-integer-unit:8, Padding/binary>>.

% string_padding(Bin) ->
%     case padding(Bin) of
%         Bin ->
%             Padding = 4 * 8,
%             <<Bin/binary, 0:Padding>>;
%         Res ->
%             Res
%     end.

% %% -------------------------------------------------------------------------------------------------
% %% binary

% encode_binary_list(List) ->
%     Len = erlang:length(List),
%     StartOffset = Len * 4,
%     StringListVector = encode_binary_in_list(StartOffset, List, {[], []}),
%     list_to_binary([<<Len:4/little-signed-integer-unit:8>>, StringListVector]).

% encode_binary_in_list(_, [], {OffsetList, BinaryList}) ->
%     [lists:reverse(OffsetList), lists:reverse(BinaryList)];
% encode_binary_in_list(StartOffset, [Binary | List], {OffsetList, BinaryList}) ->
%     BinaryEncode = encode_binary(Binary),
%     Size = erlang:size(BinaryEncode),
%     NextOffset = StartOffset + Size - 4,
%     OffsetBinary = <<StartOffset:4/little-signed-integer-unit:8>>,
%     encode_binary_in_list(NextOffset, List,
%         {[OffsetBinary | OffsetList], [ BinaryEncode| BinaryList]}).

% encode_binary(Binary) ->
%     Size = erlang:size(Binary),
%     Binary1 = padding(Binary),
%     <<Size:4/little-signed-integer-unit:8, Binary1/binary>>.

% %% -------------------------------------------------------------------------------------------------
% %% util
% to_binary(X) when is_binary(X)  -> X;
% to_binary(X) when is_list(X)    -> list_to_binary(X);
% to_binary(X) when is_integer(X) -> integer_to_binary(X);
% to_binary(X) when is_float(X)   -> float_to_binary(X, [{decimals, 10}, compact]);
% to_binary(X) when is_atom(X)    -> atom_to_binary(X, utf8).

% %% little-signed-integer-unit
% lsiu(Size) ->
%     <<Size:4/little-signed-integer-unit:8>>.

% padding(Bin) when is_binary(Bin) ->
%     Size = erlang:size(Bin),
%     Padding = (4 - (Size rem 4)) * 8,
%     <<Bin/binary, 0:Padding>>.
