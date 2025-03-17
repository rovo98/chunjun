package com.dtstack.flinkx.starrocks;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class RowUtils {
    private RowUtils() {}

    public interface TypedMapFunc<IN, OUT> extends MapFunction<IN, OUT>, ResultTypeQueryable<OUT> {
        DataType getProducedDataType();
    }

    public static List<RowData.FieldGetter> getRowDataFieldGetters(DataType rowDataType) {
        Preconditions.checkArgument(
                rowDataType.getLogicalType().getTypeRoot() == LogicalTypeRoot.ROW);
        return getRowDataFieldGetters(
                rowDataType.getChildren().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList()));
    }

    public static List<RowData.FieldGetter> getRowDataFieldGetters(RowType rowType) {
        return getRowDataFieldGetters(
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .collect(Collectors.toList()));
    }

    public static List<RowData.FieldGetter> getRowDataFieldGetters(List<LogicalType> logicalTypes) {
        List<RowData.FieldGetter> fieldGetterList = new ArrayList<>();
        for (int i = 0; i < logicalTypes.size(); i++) {
            final RowData.FieldGetter fieldGetter =
                    RowData.createFieldGetter(logicalTypes.get(i), i);
            fieldGetterList.add(fieldGetter);
        }
        return fieldGetterList;
    }

    public static void copyRowData(
            RowData input, GenericRowData output, List<RowData.FieldGetter> fieldGetters) {
        for (int i = 0; i < input.getArity() && i < output.getArity(); i++) {
            if (input instanceof GenericRowData) {
                output.setField(i, ((GenericRowData) input).getField(i));
            } else {
                Preconditions.checkArgument(fieldGetters != null);
                Object value = fieldGetters.get(i).getFieldOrNull(input);
                output.setField(i, value);
            }
        }
    }

    public static TypedMapFunc<RowData, Row> rowDataRowTypedMapFunc(DataType rowDataType) {
        LogicalType logicalType = rowDataType.getLogicalType();
        Preconditions.checkArgument(logicalType.getTypeRoot() == LogicalTypeRoot.ROW);
        return new TypedMapFunc<RowData, Row>() {

            private RowData.FieldGetter[] fieldGetters =
                    getRowDataFieldGetters(rowDataType).toArray(new RowData.FieldGetter[0]);

            @Override
            public DataType getProducedDataType() {
                return rowDataType.bridgedTo(Row.class);
            }

            @Override
            public Row map(RowData rowData) throws Exception {
                Row row = new Row(rowData.getRowKind(), rowData.getArity());
                for (int i = 0; i < rowData.getArity(); i++) {
                    RowData.FieldGetter fieldGetter = fieldGetters[i];
                    row.setField(i, fieldGetter.getFieldOrNull(rowData));
                }
                return row;
            }

            @Override
            public TypeInformation<Row> getProducedType() {
                RowType rowType = (RowType) logicalType;
                List<RowType.RowField> rowFields = rowType.getFields();
                List<DataType> rowDataTypes = rowDataType.getChildren();
                TypeInformation<?>[] fieldTypeInfos =
                        rowDataTypes.stream()
                                .map(t -> InternalTypeInfo.of(t.getLogicalType()))
                                .toArray(TypeInformation[]::new);
                String[] fieldNames =
                        rowFields.stream().map(RowType.RowField::getName).toArray(String[]::new);
                return new RowTypeInfo(fieldTypeInfos, fieldNames);
            }
        };
    }
}
