package com.dtstack.flinkx.iceberg.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.iceberg.IcebergUtil;
import com.dtstack.flinkx.iceberg.config.IcebergConfig;
import com.dtstack.flinkx.writer.BaseDataWriter;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_CATALOG_TYPE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_COLUMN_NAME;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_COLUMN_TYPE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_DATABASE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_HADOOP_CONFIG;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_HADOOP_CONF_DIR;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_HIVE_CONF_DIR;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_METASTORE_URIS;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_TABLE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_WAREHOUSE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_WRITE_MODE;

public class IcebergWriter extends BaseDataWriter {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);

    private final IcebergConfig icebergConfig;

    private WriteMode writeMode;
    private int parallelism;

    private List<String> columnNames;
    private List<String> columnTypes;

    @SuppressWarnings("unchecked")
    public IcebergWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        icebergConfig =
                IcebergConfig.builder()
                        .hadoopConfig(
                                (Map<String, Object>)
                                        writerConfig.getParameter().getVal(KEY_HADOOP_CONFIG))
                        .warehouse(writerConfig.getParameter().getStringVal(KEY_WAREHOUSE))
                        .metastoreUris(writerConfig.getParameter().getStringVal(KEY_METASTORE_URIS))
                        .database(writerConfig.getParameter().getStringVal(KEY_DATABASE))
                        .table(writerConfig.getParameter().getStringVal(KEY_TABLE))
                        .hadoopConfDir(
                                writerConfig.getParameter().getStringVal(KEY_HADOOP_CONF_DIR))
                        .hiveConfDir(writerConfig.getParameter().getStringVal(KEY_HIVE_CONF_DIR))
                        .catalogType(
                                writerConfig.getParameter().getStringVal(KEY_CATALOG_TYPE, "hive"))
                        .build();
        writeMode =
                WriteMode.of(writerConfig.getParameter().getStringVal(KEY_WRITE_MODE, "default"));
        List<?> columns = writerConfig.getParameter().getColumn();
        parallelism = config.getJob().getSetting().getSpeed().getChannel();
        Preconditions.checkState(columns != null && !columns.isEmpty(), "columns is required!");
        columnNames = new ArrayList<>();
        columnTypes = new ArrayList<>();
        for (Object column : columns) {
            Map<String, String> cm = (Map<String, String>) column;
            columnNames.add(cm.get(KEY_COLUMN_NAME));
            columnTypes.add(cm.get(KEY_COLUMN_TYPE));
        }
        LOG.info("Accepted iceberg config -> {}, writeMode? -> {}", icebergConfig, writeMode);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        TableLoader tableLoader = IcebergUtil.buildTableLoader(icebergConfig);
        TableSchema requestedTblSchema = constructRequestedTblSchema();
        Table targetTable = tableLoader.loadTable();
        Schema fullSchema = targetTable.schema();
        LOG.info("Requested table schema for input rows -> {}", requestedTblSchema.toString());
        DataStream<Row> schemaAligned =
                dataSet.map(new SchemaAlignment(fullSchema, requestedTblSchema))
                        .name("align-table-schema");
        FlinkSink.Builder sinkBuilder =
                FlinkSink.forRow(schemaAligned, FlinkSchemaUtil.toSchema(fullSchema))
                        .tableLoader(tableLoader)
                        .writeParallelism(parallelism);
        switch (writeMode) {
            case UPSERT:
                sinkBuilder.upsert(true);
                break;
            case OVERWRITE:
                sinkBuilder.overwrite(true);
                break;
            default:
                break;
        }
        return sinkBuilder.append();
    }

    private TableSchema constructRequestedTblSchema() {
        DataType[] flinkDataTypes =
                columnTypes.stream()
                        .map(IcebergUtil::internalType2FlinkDataType)
                        .toArray(DataType[]::new);
        return TableSchema.builder()
                .fields(columnNames.toArray(new String[0]), flinkDataTypes)
                .build();
    }
}
