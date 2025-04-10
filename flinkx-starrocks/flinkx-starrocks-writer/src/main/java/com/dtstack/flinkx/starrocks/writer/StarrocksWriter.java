package com.dtstack.flinkx.starrocks.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.starrocks.StarRocksUtil;
import com.dtstack.flinkx.starrocks.config.StarRocksConfig;
import com.dtstack.flinkx.writer.BaseDataWriter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.connector.flink.FXStarRocksSink;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.manager.StarRocksQueryVisitor;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.connector.flink.row.sink.StarRocksSinkRowBuilder;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.dtstack.flinkx.starrocks.config.StarRocksConfigKeys.*;
import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.DATABASE_NAME;
import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.JDBC_URL;
import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.LOAD_URL;
import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.PASSWORD;
import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.SINK_BATCH_FLUSH_INTERVAL;
import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.TABLE_NAME;
import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.USERNAME;

public class StarrocksWriter extends BaseDataWriter {
    private static final Logger LOG = LoggerFactory.getLogger(StarrocksWriter.class);

    private final StarRocksConfig starRocksConfig;

    private List<String> columnNames;
    private List<String> columnTypes;

    private List<String> preSql;
    private List<String> postSql;

    @SuppressWarnings("unchecked")
    public StarrocksWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        starRocksConfig =
                StarRocksConfig.builder()
                        .jdbcUrl(writerConfig.getParameter().getStringVal(KEY_JDBC_URL))
                        .httpUrl(writerConfig.getParameter().getStringVal(KEY_HTTP_URL))
                        .database(writerConfig.getParameter().getStringVal(KEY_DATABASE))
                        .table(writerConfig.getParameter().getStringVal(KEY_TABLE))
                        .username(writerConfig.getParameter().getStringVal(KEY_USERNAME))
                        .password(writerConfig.getParameter().getStringVal(KEY_PASSWORD))
                        .optionalProps(
                                (Map<String, String>)
                                        writerConfig.getParameter().getVal(KEY_OPTION_PROPS))
                        .build();
        List<?> columns = writerConfig.getParameter().getColumn();
        Preconditions.checkState(columns != null && !columns.isEmpty(), "columns is required!");
        columnNames = Lists.newArrayList();
        columnTypes = Lists.newArrayList();
        for (Object col : columns) {
            Map<String, String> cm = (Map<String, String>) col;
            columnNames.add(cm.get("name"));
            columnTypes.add(cm.get("type"));
        }
        preSql = (List<String>) writerConfig.getParameter().getVal("preSql");
        postSql = (List<String>) writerConfig.getParameter().getVal("postSql");
        LOG.info(
                "Accepted starrocks config -> {}.\n\npreSql: {}.\npostSql: {}",
                starRocksConfig,
                preSql,
                postSql);
    }

    private StarRocksSinkOptions genSinkOptions(boolean presentPks) {
        StarRocksSinkOptions.Builder b = StarRocksSinkOptions.builder();
        String sinkColumns = String.join(",", columnNames) + (presentPks ? ",__op" : "");
        b.withProperty(JDBC_URL.key(), starRocksConfig.getJdbcUrl())
                .withProperty(LOAD_URL.key(), starRocksConfig.getHttpUrl().replaceAll(",", ";"))
                .withProperty(DATABASE_NAME.key(), starRocksConfig.getDatabase())
                .withProperty(TABLE_NAME.key(), starRocksConfig.getTable())
                .withProperty(USERNAME.key(), starRocksConfig.getUsername())
                .withProperty(PASSWORD.key(), starRocksConfig.getPassword())
                .withProperty(SINK_BATCH_FLUSH_INTERVAL.key(), "60000") // 1 min
                .withProperty("sink.properties.columns", sinkColumns);
        if (presentPks) {
            b.withProperty("sink.properties.partial_update", "true");
        }
        // setup optional properties
        if (starRocksConfig.getOptionalProps() != null) {
            starRocksConfig.getOptionalProps().forEach(b::withProperty);
        }
        return b.build();
    }

    private TableSchema constructFlinkSchema() {
        DataType[] flinkDataTypes =
                columnTypes.stream()
                        .map(StarRocksUtil::internalType2FlinkDataType)
                        .toArray(DataType[]::new);
        Set<String> pks = probePrimaryKeys();
        LOG.info("Probed primary keys: {}.", pks);
        TableSchema.Builder b = TableSchema.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            String cn = columnNames.get(i);
            if (!pks.isEmpty() && pks.contains(cn)) {
                b.field(cn, flinkDataTypes[i].notNull());
            } else {
                b.field(cn, flinkDataTypes[i]);
            }
        }
        if (!pks.isEmpty()) {
            b.primaryKey(pks.toArray(new String[0]));
        }
        TableSchema schema = b.build();
        LOG.info("Sink schema -> {}.", schema);
        return schema;
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        TableSchema schema = constructFlinkSchema();
        boolean presentPks = schema.getPrimaryKey().isPresent();
        SinkFunction<Row> starRockSink =
                FXStarRocksSink.sink(
                        schema,
                        genSinkOptions(presentPks),
                        new RowTransformer(presentPks),
                        preSql,
                        postSql);
        return dataSet.addSink(starRockSink).name(this.getClass().getSimpleName().toLowerCase());
    }

    private Set<String> probePrimaryKeys() {
        Set<String> pks = Sets.newHashSet();
        StarRocksJdbcConnectionProvider jdbcConnProvider =
                new StarRocksJdbcConnectionProvider(
                        new StarRocksJdbcConnectionOptions(
                                this.starRocksConfig.getJdbcUrl(),
                                this.starRocksConfig.getUsername(),
                                this.starRocksConfig.getPassword()));
        final String database = this.starRocksConfig.getDatabase();
        final String table = this.starRocksConfig.getTable();
        StarRocksQueryVisitor starRocksQueryVisitor =
                new StarRocksQueryVisitor(jdbcConnProvider, database, table);
        List<Map<String, Object>> rows = starRocksQueryVisitor.getTableColumnsMetaData();
        for (Map<String, Object> row : rows) {
            String keysType = row.get("COLUMN_KEY").toString();
            if (!"PRI".equals(keysType)) {
                continue;
            }
            pks.add(row.get("COLUMN_NAME").toString().toLowerCase());
        }
        return pks;
    }

    private static class RowTransformer implements StarRocksSinkRowBuilder<Row> {

        private final boolean handlePKs;

        public RowTransformer(boolean handlePKs) {
            this.handlePKs = handlePKs;
        }

        @Override
        public void accept(Object[] internalRow, Row row) {
            int dLen = handlePKs ? internalRow.length - 1 : internalRow.length;
            Preconditions.checkState(
                    dLen <= row.getArity(),
                    "sink column count mismatch, expected: %s, got: %s.",
                    dLen,
                    row.getArity());
            for (int i = 0; i < dLen; i++) {
                internalRow[i] = row.getField(i);
            }
            // When the StarRocks table is a Primary Key table, you need to set
            // the last element to indicate whether the data loading is an UPSERT or DELETE
            // operation
            if (handlePKs) {
                internalRow[internalRow.length - 1] = StarRocksSinkOP.UPSERT.ordinal();
            }
        }
    }
}
