package com.dtstack.flinkx.iceberg.reader;

import com.dtstack.flinkx.iceberg.IcebergUtil;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;

import com.google.common.base.Preconditions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkInputSplit;
import org.apache.iceberg.flink.source.FlinkSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

public class IcebergInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

    private final TableLoader tableLoader;
    private final List<MetaColumn> projectedColumns;

    private Table table;
    private FlinkInputFormat flinkInputFormat;

    private DataStructureConverter<Object, Object> rd2rConverter;

    public IcebergInputFormat(
            TableLoader tableLoader, List<MetaColumn> metaColumns, List<Expression> filters) {
        Preconditions.checkNotNull(tableLoader, "tableLoader must be configured");
        this.tableLoader = tableLoader;
        this.projectedColumns = metaColumns;

        if (!tableLoader.isOpen()) {
            this.tableLoader.open();
        }
        this.table = tableLoader.loadTable();
        FlinkSource.Builder sourceBuilder =
                FlinkSource.forRowData().tableLoader(tableLoader).streaming(false);
        // config projection fields
        if (projectedColumns != null && !projectedColumns.isEmpty()) {
            sourceBuilder.project(constructProjectSchema());
        }
        // config filters for scan context.
        if (filters != null && !filters.isEmpty()) {
            sourceBuilder.filters(filters);
        }
        this.flinkInputFormat = sourceBuilder.buildFormat();
        // construct RowData to Row converter
        RowType rowType = FlinkSchemaUtil.convert(this.table.schema());
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
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("open inputFormat:> inputSplit -:> {}", inputSplit);
        this.flinkInputFormat.open((FlinkInputSplit) inputSplit);
    }

    private TableSchema constructProjectSchema() {
        List<String> names = new ArrayList<>();
        List<DataType> datatypes = new ArrayList<>();
        for (MetaColumn mc : projectedColumns) {
            names.add(mc.getName());
            datatypes.add(IcebergUtil.internalType2FlinkDataType(mc.getType()));
        }
        TableSchema tblSchema =
                TableSchema.builder()
                        .fields(names.toArray(new String[0]), datatypes.toArray(new DataType[0]))
                        .build();
        LOG.info("projected table schema :> {}", tblSchema);
        return tblSchema;
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int i) throws Exception {
        return this.flinkInputFormat.createInputSplits(i);
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        RowData rowData = this.flinkInputFormat.nextRecord(null);
        return (Row) rd2rConverter.toExternal(rowData);
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
