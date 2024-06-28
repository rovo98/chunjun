package com.dtstack.flinkx.iceberg.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;

import com.dtstack.flinkx.reader.MetaColumn;
import com.google.common.base.Preconditions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkSource;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

public class IcebergInputFormat extends BaseRichInputFormat {

    private final TableLoader tableLoader;
    private final List<MetaColumn> metaColumns;

    private Table table;
    private FlinkInputFormat flinkInputFormat;

    private DataStructureConverter<Object, Object> rd2rConverter;

    public IcebergInputFormat(TableLoader tableLoader, List<MetaColumn> metaColumns) {
        this.tableLoader = tableLoader;
        this.metaColumns = metaColumns;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        Preconditions.checkNotNull(tableLoader, "tableLoader must be configured");
        if (!tableLoader.isOpen()) {
            this.tableLoader.open();
        }
        this.table = tableLoader.loadTable();
        this.flinkInputFormat =
                FlinkSource.forRowData().tableLoader(tableLoader).streaming(false).buildFormat();
        RowType rowType = FlinkSchemaUtil.convert(this.table.schema());

        // construct RowData to Row converter
        DataTypes.Field[] fields =
                rowType.getFields().stream()
                        .map(
                                f ->
                                        FIELD(
                                                f.getName(),
                                                TypeConversions.fromLogicalToDataType(f.getType())))
                        .toArray(DataTypes.Field[]::new);
        rd2rConverter = DataStructureConverters.getConverter(ROW(fields));
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int i) throws Exception {
        return this.flinkInputFormat.createInputSplits(i);
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        RowData rowData = this.flinkInputFormat.nextRecord(null);
        return fieldProjection((Row) rd2rConverter.toExternal(rowData));
    }

    private Row fieldProjection(Row row) {
        Row projectedRow = new Row(metaColumns.size());
        for (int i = 0; i < metaColumns.size(); i++) {
            MetaColumn mc = metaColumns.get(i);
            Object value = null;
            if (mc.getValue() != null) {
                value = mc.getValue();
            } else if (mc.getIndex() != null && mc.getIndex() < row.getArity()) {
                value = row.getField(mc.getIndex());
            }
            projectedRow.setField(i, value);
        }
        return projectedRow;
    }

    @Override
    protected void closeInternal() throws IOException {
        this.flinkInputFormat.close();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return this.flinkInputFormat.reachedEnd();
    }
}
