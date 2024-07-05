package com.dtstack.flinkx.iceberg.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.iceberg.IcebergUtil;
import com.dtstack.flinkx.iceberg.config.IcebergConfig;
import com.dtstack.flinkx.writer.BaseDataWriter;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_COLUMN_NAME;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_COLUMN_TYPE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_DATABASE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_HADOOP_CONFIG;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_METASTORE_URIS;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_TABLE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_WAREHOUSE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_WRITE_MODE;

public class IcebergWriter extends BaseDataWriter {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);

    private final IcebergConfig icebergConfig;

    private final boolean isOverwrite;
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
                        .build();
        isOverwrite =
                writerConfig
                        .getParameter()
                        .getStringVal(KEY_WRITE_MODE)
                        .equalsIgnoreCase("overwrite");
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
        LOG.info("Accepted iceberg config -> {}, overwrite? -> {}", icebergConfig, isOverwrite);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        TableLoader tableLoader = IcebergUtil.buildTableLoader(icebergConfig);
        Table table = tableLoader.loadTable();
        // NOTE: Add dummy sink to collect output metrics for FlinkX
        IcebergOutputFormat outputFormat = new IcebergOutputFormat();
        createOutput(dataSet, outputFormat, "metrics-collection-dummy-sink").setParallelism(1);
        //
        return FlinkSink.forRow(dataSet, FlinkSchemaUtil.toSchema(table.schema()))
                .tableLoader(tableLoader)
                .writeParallelism(parallelism)
                .overwrite(isOverwrite)
                .equalityFieldColumns(columnNames)
                .flinkXMetrics(outputFormat.getFlinkXBaseMetric())
                .append();
    }
}
