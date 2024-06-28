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
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_METASTORE_URIS;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_TABLE;
import static com.dtstack.flinkx.iceberg.config.IcebergConfigKeys.KEY_WAREHOUSE;

public class IcebergReader extends BaseDataReader {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergReader.class);
    private final IcebergConfig icebergConfig;

    private List<MetaColumn> metaColumns;

    @SuppressWarnings("unchecked")
    protected IcebergReader(DataTransferConfig config, StreamExecutionEnvironment env) {
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
                        .build();
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn(), false);
        LOG.info("Accepted iceberg config -> {}, metaColumns size -> {}", icebergConfig, metaColumns.size());
    }

    @Override
    public DataStream<Row> readData() {
        return createInput(new IcebergInputFormat(IcebergUtil.buildTableLoader(icebergConfig), metaColumns));
    }
}
