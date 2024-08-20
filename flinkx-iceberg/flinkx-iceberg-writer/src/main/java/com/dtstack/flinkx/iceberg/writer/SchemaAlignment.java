package com.dtstack.flinkx.iceberg.writer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

public class SchemaAlignment implements MapFunction<Row, Row> {
    private static final long serialVersionUID = 1L;

    private final Schema fullTableSchema;
    private final Map<String, Integer> rowFn2IndexMap;

    public SchemaAlignment(Schema fullTableSchema, TableSchema requestedTblSchema) {
        this.fullTableSchema = fullTableSchema;
        rowFn2IndexMap = new HashMap<>(requestedTblSchema.getFieldCount());
        int i = 0;
        for (String fn : requestedTblSchema.getFieldNames()) {
            rowFn2IndexMap.put(fn, i++);
        }
    }

    @Override
    public Row map(Row value) throws Exception {
        Row rowData = new Row(value.getKind(), fullTableSchema.columns().size());
        int i = 0;
        for (Types.NestedField tf : fullTableSchema.columns()) {
            if (rowFn2IndexMap.containsKey(tf.name())) {
                rowData.setField(i++, value.getField(rowFn2IndexMap.get(tf.name())));
            } else {
                rowData.setField(i++, null);
            }
        }
        return rowData;
    }
}
