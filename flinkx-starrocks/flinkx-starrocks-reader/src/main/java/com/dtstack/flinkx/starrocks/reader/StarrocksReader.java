package com.dtstack.flinkx.starrocks.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.starrocks.RowUtils;
import com.dtstack.flinkx.starrocks.StarRocksUtil;
import com.dtstack.flinkx.starrocks.config.StarRocksConfig;

import com.google.common.base.Preconditions;
import com.starrocks.connector.flink.StarRocksSource;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.manager.StarRocksQueryVisitor;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.dtstack.flinkx.starrocks.config.StarRocksConfigKeys.*;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.DATABASE_NAME;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.JDBC_URL;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.PASSWORD;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.SCAN_COLUMNS;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.SCAN_FILTER;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.SCAN_URL;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.TABLE_NAME;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.USERNAME;

public class StarrocksReader extends BaseDataReader {
    private static final Logger LOG = LoggerFactory.getLogger(StarrocksReader.class);

    private StarRocksConfig starRocksConfig;
    private List<MetaColumn> projectColumns;
    private String filterClause;

    private StarRocksQueryVisitor starRocksQueryVisitor;

    @SuppressWarnings("unchecked")
    public StarrocksReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        starRocksConfig =
                StarRocksConfig.builder()
                        .jdbcUrl(readerConfig.getParameter().getStringVal(KEY_JDBC_URL))
                        .httpUrl(readerConfig.getParameter().getStringVal(KEY_HTTP_URL))
                        .database(readerConfig.getParameter().getStringVal(KEY_DATABASE))
                        .table(readerConfig.getParameter().getStringVal(KEY_TABLE))
                        .username(readerConfig.getParameter().getStringVal(KEY_USERNAME))
                        .password(readerConfig.getParameter().getStringVal(KEY_PASSWORD))
                        .optionalProps(
                                (Map<String, String>)
                                        readerConfig.getParameter().getVal(KEY_OPTION_PROPS))
                        .build();
        projectColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn(), false);
        filterClause = readerConfig.getParameter().getStringVal("where", "");

        LOG.info("Accepted starRocks config -> {}", starRocksConfig);
        Preconditions.checkState(!projectColumns.isEmpty(), "Project columns can NOT be empty!");

        //
        StarRocksJdbcConnectionProvider jdbcConnProvider =
                new StarRocksJdbcConnectionProvider(
                        new StarRocksJdbcConnectionOptions(
                                this.starRocksConfig.getJdbcUrl(),
                                this.starRocksConfig.getUsername(),
                                this.starRocksConfig.getPassword()));
        final String database = this.starRocksConfig.getDatabase();
        final String table = this.starRocksConfig.getTable();
        starRocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnProvider, database, table);
    }

    private TableSchema constructFlinkSchema() {
        List<Map<String, Object>> metadata = starRocksQueryVisitor.getTableColumnsMetaData();
        Map<String, String> preciseDecimalCols = probeDecimalColTypes(metadata);
        List<String> names = new ArrayList<>();
        List<DataType> datatypes = new ArrayList<>();
        for (MetaColumn mc : projectColumns) {
            names.add(mc.getName());
            datatypes.add(StarRocksUtil.internalType2FlinkDataType(mc.getType()));
        }
        TableSchema.Builder b = TableSchema.builder();

        for (int i = 0; i < names.size(); i++) {
            String cn = names.get(i).toLowerCase();
            DataType dt = datatypes.get(i);
            if (dt == null) {
                LOG.info("Unknown precision & scale decimal type encountered -> col:`{}`.", cn);
                String probedDT = preciseDecimalCols.get(cn);
                Preconditions.checkNotNull(
                        probedDT, "Failed to be precision & scale of Decimal col - " + cn);
                dt = StarRocksUtil.toFlinkDecimalType(probedDT);
            }
            b.field(cn, dt);
        }
        TableSchema schema = b.build();
        LOG.info("projected table schema -> {}", schema);
        return schema;
    }

    private Map<String, String> probeDecimalColTypes(List<Map<String, Object>> rows) {
        Map<String, String> result = new HashMap<>();
        for (Map<String, Object> row : rows) {
            String dataType = row.get("DATA_TYPE").toString();
            if (dataType.equalsIgnoreCase("DECIMAL")) {
                String column = row.get("COLUMN_NAME").toString().toLowerCase();
                String precision = row.get("COLUMN_SIZE").toString();
                String scale = row.get("DECIMAL_DIGITS").toString();
                result.put(column, String.format("%s(%s,%s)", dataType, precision, scale));
            }
        }
        return result;
    }

    private StarRocksSourceOptions genSourceOptions() {
        StarRocksSourceOptions.Builder b = StarRocksSourceOptions.builder();
        b.withProperty(JDBC_URL.key(), starRocksConfig.getJdbcUrl())
                .withProperty(SCAN_URL.key(), starRocksConfig.getHttpUrl())
                .withProperty(DATABASE_NAME.key(), starRocksConfig.getDatabase())
                .withProperty(TABLE_NAME.key(), starRocksConfig.getTable())
                .withProperty(USERNAME.key(), starRocksConfig.getUsername())
                .withProperty(PASSWORD.key(), starRocksConfig.getPassword())
                .withProperty(
                        SCAN_COLUMNS.key(),
                        projectColumns.stream()
                                .map(MetaColumn::getName)
                                .collect(Collectors.joining(",")));
        if (StringUtils.isNotBlank(filterClause)) {
            b.withProperty(SCAN_FILTER.key(), filterClause);
        }
        return b.build();
    }

    private DataType buildRowType(DataType... fieldDataTypes) {
        return DataTypes.ROW(
                IntStream.range(0, fieldDataTypes.length)
                        .mapToObj(idx -> DataTypes.FIELD("f" + idx, fieldDataTypes[idx]))
                        .toArray(DataTypes.Field[]::new));
    }

    @Override
    public DataStream<Row> readData() {
        TableSchema schema = constructFlinkSchema();
        SourceFunction<RowData> starRockSource = StarRocksSource.source(schema, genSourceOptions());

        return env.addSource(starRockSource, this.getClass().getSimpleName().toLowerCase())
                .map(RowUtils.rowDataRowTypedMapFunc(buildRowType(schema.getFieldDataTypes())))
                .name("rd2r");
    }
}
