package com.starrocks.connector.flink.table.sink;

import com.dtstack.flinkx.constants.Metrics;

import com.google.common.base.Strings;
import com.starrocks.connector.flink.OperationHook;
import com.starrocks.connector.flink.SinkHookOpsManager;
import com.starrocks.connector.flink.StarRocksOpsHook;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.manager.StarRocksSinkManagerV2;
import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.tools.EnvUtils;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.truncate.Truncate;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class FXStarRocksDynamicSinkFunctionV2<T> extends StarRocksDynamicSinkFunctionBase<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger log =
            LoggerFactory.getLogger(FXStarRocksDynamicSinkFunctionV2.class);

    private static final int NESTED_ROW_DATA_HEADER_SIZE = 256;

    private final StarRocksSinkOptions sinkOptions;
    private final StarRocksSinkManagerV2 sinkManager;
    private final StarRocksISerializer serializer;
    private final StarRocksIRowTransformer<T> rowTransformer;

    private transient volatile ListState<StarrocksSnapshotState> snapshotStates;
    private final Map<Long, List<StreamLoadSnapshot>> snapshotMap = new ConcurrentHashMap<>();

    @Deprecated private transient ListState<Map<String, StarRocksSinkBufferEntity>> legacyState;
    @Deprecated private transient List<StarRocksSinkBufferEntity> legacyData;

    // Fit FlinkX features.
    private List<String> preSql = null;
    private List<String> postSql = null;
    private SinkHookOpsManager sinkHookOpsManager;

    // Adapt FlinkX metrics collection feature
    private LongCounter numWriteCounter;
    private LongCounter bytesWriteCounter;
    private LongCounter durationCounter;
    private long startTime;

    /*
    indicators for indicating job's current executing operation.
    */
    private IntCounter dataPreprocessIndicator;
    private IntCounter dataSyncIndicator;
    private IntCounter dataPostProcessIndicator;
    //

    public FXStarRocksDynamicSinkFunctionV2(
            StarRocksSinkOptions sinkOptions,
            TableSchema schema,
            StarRocksIRowTransformer<T> rowTransformer,
            List<String> preSql,
            List<String> postSql) {
        this.sinkOptions = sinkOptions;
        this.rowTransformer = rowTransformer;
        StarRocksSinkTable sinkTable =
                StarRocksSinkTable.builder().sinkOptions(sinkOptions).build();
        sinkTable.validateTableStructure(sinkOptions, schema);
        // StarRocksJsonSerializer depends on SinkOptions#supportUpsertDelete which is decided in
        // StarRocksSinkTable#validateTableStructure, so create serializer after validating table
        // structure
        this.serializer =
                StarRocksSerializerFactory.createSerializer(sinkOptions, schema.getFieldNames());
        rowTransformer.setStarRocksColumns(sinkTable.getFieldMapping());
        rowTransformer.setTableSchema(schema);
        this.sinkManager =
                new StarRocksSinkManagerV2(
                        sinkOptions.getProperties(),
                        sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE);
        this.preSql = preSql;
        this.postSql = postSql;
    }

    public FXStarRocksDynamicSinkFunctionV2(StarRocksSinkOptions sinkOptions) {
        this.sinkOptions = sinkOptions;
        this.sinkManager =
                new StarRocksSinkManagerV2(
                        sinkOptions.getProperties(),
                        sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE);
        this.serializer = null;
        this.rowTransformer = null;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (serializer == null) {
            if (value instanceof StarRocksSinkRowDataWithMeta) {
                StarRocksSinkRowDataWithMeta data = (StarRocksSinkRowDataWithMeta) value;
                if (Strings.isNullOrEmpty(data.getDatabase())
                        || Strings.isNullOrEmpty(data.getTable())
                        || data.getDataRows() == null) {
                    log.warn(
                            String.format(
                                    "json row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                                    data.getDatabase(),
                                    data.getTable(),
                                    Arrays.toString(data.getDataRows())));
                    return;
                }
                sinkManager.write(null, data.getDatabase(), data.getTable(), data.getDataRows());
                updateFlinkXMetrics(value.toString());
                return;
            } else if (value instanceof StarRocksRowData) {
                StarRocksRowData data = (StarRocksRowData) value;
                if (Strings.isNullOrEmpty(data.getDatabase())
                        || Strings.isNullOrEmpty(data.getTable())
                        || data.getRow() == null) {
                    log.warn(
                            String.format(
                                    "json row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                                    data.getDatabase(), data.getTable(), data.getRow()));
                    return;
                }
                sinkManager.write(
                        data.getUniqueKey(), data.getDatabase(), data.getTable(), data.getRow());
                updateFlinkXMetrics(value.toString());
                return;
            }
            // raw data sink
            sinkManager.write(
                    null,
                    sinkOptions.getDatabaseName(),
                    sinkOptions.getTableName(),
                    value.toString());
            updateFlinkXMetrics(value.toString());
            return;
        }

        if (value instanceof NestedRowData) {
            NestedRowData ddlData = (NestedRowData) value;
            if (ddlData.getSegments().length != 1
                    || ddlData.getSegments()[0].size() < NESTED_ROW_DATA_HEADER_SIZE) {
                return;
            }

            int totalSize = ddlData.getSegments()[0].size();
            byte[] data = new byte[totalSize - NESTED_ROW_DATA_HEADER_SIZE];
            ddlData.getSegments()[0].get(NESTED_ROW_DATA_HEADER_SIZE, data);
            Map<String, String> ddlMap =
                    InstantiationUtil.deserializeObject(data, HashMap.class.getClassLoader());
            if (ddlMap == null
                    || "true".equals(ddlMap.get("snapshot"))
                    || Strings.isNullOrEmpty(ddlMap.get("ddl"))
                    || Strings.isNullOrEmpty(ddlMap.get("databaseName"))) {
                return;
            }
            Statement statement = CCJSqlParserUtil.parse(ddlMap.get("ddl"));
            if (statement instanceof Truncate) {
                Truncate truncate = (Truncate) statement;
                if (!sinkOptions.getTableName().equalsIgnoreCase(truncate.getTable().getName())) {
                    return;
                }
                // TODO: add ddl to queue
            } else if (statement instanceof Alter) {

            }
        }
        if (value instanceof RowData) {
            if (RowKind.UPDATE_BEFORE.equals(((RowData) value).getRowKind())) {
                // do not need update_before, cauz an update action happened on the primary keys
                // will be separated into `delete` and `create`
                return;
            }
            if (!sinkOptions.supportUpsertDelete()
                    && RowKind.DELETE.equals(((RowData) value).getRowKind())) {
                // let go the UPDATE_AFTER and INSERT rows for tables who have a group of `unique`
                // or `duplicate` keys.
                return;
            }
        }
        flushLegacyData();
        sinkManager.write(
                null,
                sinkOptions.getDatabaseName(),
                sinkOptions.getTableName(),
                serializer.serialize(
                        rowTransformer.transform(value, sinkOptions.supportUpsertDelete())));
        updateFlinkXMetrics(value.toString());
    }

    private void updateFlinkXMetrics(String val) {
        // Update FlinkX metrics
        numWriteCounter.add(1);
        bytesWriteCounter.add(val.getBytes().length);

        durationCounter.resetLocal();
        durationCounter.add(System.currentTimeMillis() - startTime);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sinkManager.init();
        sinkManager.setRuntimeContext(getRuntimeContext(), sinkOptions);
        if (rowTransformer != null) {
            rowTransformer.setRuntimeContext(getRuntimeContext());
        }
        notifyCheckpointComplete(Long.MAX_VALUE);
        log.info("Open sink function v2. {}", EnvUtils.getGitInformation());

        // For FlinkX features.
        int taskNum = getRuntimeContext().getIndexOfThisSubtask();
        StarRocksJdbcConnectionOptions jdbcConnectionOptions =
                new StarRocksJdbcConnectionOptions(
                        sinkOptions.getJdbcUrl(),
                        sinkOptions.getUsername(),
                        sinkOptions.getPassword());
        OperationHook opsHook =
                new StarRocksOpsHook(taskNum, preSql, postSql, jdbcConnectionOptions);
        sinkHookOpsManager = new SinkHookOpsManager(opsHook, getRuntimeContext());
        //
        initStatisticsAccumulator();
        dataPreprocessIndicator.add(1);
        log.info("Wait performing #opsRunBeforeWrite, taskNum#{}", taskNum);
        sinkHookOpsManager.opsRunBeforeWrite();
        dataSyncIndicator.add(1);
    }

    private void initStatisticsAccumulator() {
        numWriteCounter = getRuntimeContext().getLongCounter(Metrics.NUM_WRITES);
        bytesWriteCounter = getRuntimeContext().getLongCounter(Metrics.WRITE_BYTES);
        durationCounter = getRuntimeContext().getLongCounter(Metrics.WRITE_DURATION);
        // for indicate job current operations
        /*
        if the value of the counter is positive, which indicate the corresponding operation
        is doing or has been done.
        */
        dataPreprocessIndicator = getRuntimeContext().getIntCounter("indicator#pre");
        dataSyncIndicator = getRuntimeContext().getIntCounter("indicator#sync");
        dataPostProcessIndicator = getRuntimeContext().getIntCounter("indicator#post");
        startTime = System.currentTimeMillis();
    }

    public void finish() {
        sinkManager.flush();
    }

    @Override
    public void close() {
        try {
            sinkManager.flush();
        } catch (Exception e) {
            log.error("Failed to flush when closing", e);
            throw e;
        } finally {
            StreamLoadSnapshot snapshot = sinkManager.snapshot();
            sinkManager.abort(snapshot);
            sinkManager.close();
        }
        // For FlinkX features.
        dataPostProcessIndicator.add(1);
        log.info(
                "Wait performing #opsRunBeforeClsoe, taskNum#{}",
                getRuntimeContext().getIndexOfThisSubtask());
        sinkHookOpsManager.opsRunBeforeClose();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        sinkManager.flush();
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return;
        }

        StreamLoadSnapshot snapshot = sinkManager.snapshot();

        if (sinkManager.prepare(snapshot)) {
            snapshotMap.put(
                    functionSnapshotContext.getCheckpointId(), Collections.singletonList(snapshot));

            snapshotStates.clear();
            snapshotStates.add(StarrocksSnapshotState.of(snapshotMap));
        } else {
            sinkManager.abort(snapshot);
            throw new RuntimeException("Snapshot state failed by prepare");
        }

        if (legacyState != null) {
            legacyState.clear();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
            throws Exception {
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return;
        }

        ListStateDescriptor<byte[]> descriptor =
                new ListStateDescriptor<>(
                        "starrocks-sink-transaction",
                        TypeInformation.of(new TypeHint<byte[]>() {}));

        ListState<byte[]> listState =
                functionInitializationContext.getOperatorStateStore().getListState(descriptor);
        snapshotStates =
                new SimpleVersionedListState<>(listState, new StarRocksVersionedSerializer());

        // old version
        ListStateDescriptor<Map<String, StarRocksSinkBufferEntity>> legacyDescriptor =
                new ListStateDescriptor<>(
                        "buffered-rows",
                        TypeInformation.of(
                                new TypeHint<Map<String, StarRocksSinkBufferEntity>>() {}));
        legacyState =
                functionInitializationContext
                        .getOperatorStateStore()
                        .getListState(legacyDescriptor);

        if (functionInitializationContext.isRestored()) {
            for (StarrocksSnapshotState state : snapshotStates.get()) {
                for (Map.Entry<Long, List<StreamLoadSnapshot>> entry : state.getData().entrySet()) {
                    snapshotMap.compute(
                            entry.getKey(),
                            (k, v) -> {
                                if (v == null) {
                                    return new ArrayList<>(entry.getValue());
                                }
                                v.addAll(entry.getValue());
                                return v;
                            });
                }
            }

            legacyData = new ArrayList<>();
            for (Map<String, StarRocksSinkBufferEntity> entry : legacyState.get()) {
                legacyData.addAll(entry.values());
            }
            log.info("There are {} items from legacy state", legacyData.size());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

        boolean succeed = true;

        List<Long> commitCheckpointIds =
                snapshotMap.keySet().stream()
                        .filter(cpId -> cpId <= checkpointId)
                        .sorted(Long::compare)
                        .collect(Collectors.toList());

        for (Long cpId : commitCheckpointIds) {
            try {
                for (StreamLoadSnapshot snapshot : snapshotMap.get(cpId)) {
                    if (!sinkManager.commit(snapshot)) {
                        succeed = false;
                        break;
                    }
                }

                if (!succeed) {
                    throw new RuntimeException(
                            String.format(
                                    "Failed to commit some transactions for snapshot %s, "
                                            + "please check taskmanager logs for details",
                                    cpId));
                }
            } catch (Exception e) {
                log.error(
                        "Failed to notify checkpoint complete, checkpoint id : {}",
                        checkpointId,
                        e);
                throw new RuntimeException(
                        "Failed to notify checkpoint complete for checkpoint id " + checkpointId,
                        e);
            }

            snapshotMap.remove(cpId);
        }

        // set legacyState to null to avoid clear it in latter snapshotState
        legacyState = null;
    }

    private void flushLegacyData() {
        if (legacyData == null || legacyData.isEmpty()) {
            return;
        }

        for (StarRocksSinkBufferEntity entity : legacyData) {
            for (byte[] data : entity.getBuffer()) {
                sinkManager.write(
                        null,
                        entity.getDatabase(),
                        entity.getTable(),
                        new String(data, StandardCharsets.UTF_8));
            }
            log.info(
                    "Write {} legacy records from table '{}' of database '{}'",
                    entity.getBuffer().size(),
                    entity.getDatabase(),
                    entity.getTable());
        }
        legacyData.clear();
    }
}
