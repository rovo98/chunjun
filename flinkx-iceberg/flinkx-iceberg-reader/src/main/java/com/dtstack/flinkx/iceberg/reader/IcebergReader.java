package com.dtstack.flinkx.iceberg.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.iceberg.IcebergUtil;
import com.dtstack.flinkx.iceberg.config.IcebergConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

    private List<Expression> filters = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public IcebergReader(DataTransferConfig config, StreamExecutionEnvironment env)
            throws JSQLParserException {
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
        final String where = readerConfig.getParameter().getStringVal(KEY_WHERE, "");
        LOG.info(
                "Accepted iceberg config -> {}, metaColumns size -> {}, where clause -> {}.",
                icebergConfig,
                projectColumns.size(),
                where);
        if (StringUtils.isNotBlank(where)) {
            filters = IcebergUtil.parseSQLFilters(where);
        }
    }

    @Override
    public DataStream<Row> readData() {
        return createInput(
                new IcebergInputFormat(
                        IcebergUtil.buildTableLoader(icebergConfig), projectColumns, filters));
    }
}
