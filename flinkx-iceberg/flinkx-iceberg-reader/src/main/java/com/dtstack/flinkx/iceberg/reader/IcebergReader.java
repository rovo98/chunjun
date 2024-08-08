package com.dtstack.flinkx.iceberg.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.iceberg.IcebergUtil;
import com.dtstack.flinkx.iceberg.config.IcebergConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_DATABASE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_HADOOP_CONFIG;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_HADOOP_CONF_DIR;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_HIVE_CONF_DIR;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_METASTORE_URIS;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_TABLE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_WAREHOUSE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_WHERE;

public class IcebergReader extends BaseDataReader {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergReader.class);
    private final IcebergConfig icebergConfig;

    private List<MetaColumn> projectColumns;

    private String filterClause;

    @SuppressWarnings("unchecked")
    public IcebergReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        icebergConfig =
                IcebergConfig.builder()
                        .hadoopConfig(
                                (Map<String, Object>)
                                        readerConfig.getParameter().getVal(KEY_HADOOP_CONFIG))
                        .metastoreUris(readerConfig.getParameter().getStringVal(KEY_METASTORE_URIS))
                        .warehouse(readerConfig.getParameter().getStringVal(KEY_WAREHOUSE))
                        .database(readerConfig.getParameter().getStringVal(KEY_DATABASE))
                        .table(readerConfig.getParameter().getStringVal(KEY_TABLE))
                        .hadoopConfDir(
                                readerConfig.getParameter().getStringVal(KEY_HADOOP_CONF_DIR))
                        .hiveConfDir(readerConfig.getParameter().getStringVal(KEY_HIVE_CONF_DIR))
                        .build();
        projectColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn(), false);
        filterClause = readerConfig.getParameter().getStringVal(KEY_WHERE, "");
        LOG.info(
                "Accepted iceberg config -> {}, metaColumns size -> {}, where clause -> {}.",
                icebergConfig,
                projectColumns.size(),
                filterClause);
    }

    @Override
    public DataStream<Row> readData() {
        IcebergInputFormat inputFormat =
                new IcebergInputFormat(
                        IcebergUtil.buildTableLoader(icebergConfig), projectColumns, filterClause);
        inputFormat.setDataTransferConfig(dataTransferConfig);
        inputFormat.setLogConfig(logConfig);
        inputFormat.setTestConfig(testConfig);
        inputFormat.setRestoreConfig(restoreConfig);
        return createInput(inputFormat);
    }
}
