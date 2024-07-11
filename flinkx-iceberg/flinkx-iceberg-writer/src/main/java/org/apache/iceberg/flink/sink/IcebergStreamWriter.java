/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink;

import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.iceberg.writer.FlinkXBaseMetricsWaitBarrier;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

class IcebergStreamWriter<T> extends AbstractStreamOperator<WriteResult>
        implements OneInputStreamOperator<T, WriteResult>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final String fullTableName;
    private final TaskWriterFactory<T> taskWriterFactory;

    private transient TaskWriter<T> writer;
    private transient int subTaskId;
    private transient int attemptId;
    private transient IcebergStreamWriterMetrics writerMetrics;
    private transient FlinkXBaseMetricsWaitBarrier flinkXBaseMetricsWaitBarrier;

    private boolean handleFlinkXOutputMetrics = false;

    IcebergStreamWriter(
            String fullTableName,
            TaskWriterFactory<T> taskWriterFactory,
            FlinkXBaseMetricsWaitBarrier metricsWaitBarrier) {
        this.fullTableName = fullTableName;
        this.taskWriterFactory = taskWriterFactory;
        this.flinkXBaseMetricsWaitBarrier = metricsWaitBarrier;
        this.handleFlinkXOutputMetrics = Objects.nonNull(this.flinkXBaseMetricsWaitBarrier);
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void open() {
        this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.attemptId = getRuntimeContext().getAttemptNumber();
        this.writerMetrics = new IcebergStreamWriterMetrics(super.metrics, fullTableName);

        // Initialize the task writer factory.
        this.taskWriterFactory.initialize(subTaskId, attemptId);

        // Initialize the task writer.
        this.writer = taskWriterFactory.create();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        flush();
        this.writer = taskWriterFactory.create();
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        writer.write(element.getValue());
        if (handleFlinkXOutputMetrics) {
            flinkXBaseMetricsWaitBarrier.waitWhileMetricsInitialized();
            updateFlinkXOutputMetrics();
        }
    }

    private void updateFlinkXOutputMetrics() {
        Preconditions.checkState(
                flinkXBaseMetricsWaitBarrier.getMetrics() != null,
                "FlinkX base metrics is not ready to use!");
        LongCounter numWrites =
                flinkXBaseMetricsWaitBarrier
                        .getMetrics()
                        .getMetricCounters()
                        .get(Metrics.NUM_WRITES);
        if (numWrites != null) {
            numWrites.add(1);
        } else {
            LOG.error("FlinkX base metric-> NUM_WRITES is null. Might not initialized!");
            throw new IllegalStateException(
                    "The FlinkX base metrics might not initialized successfully!");
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    @Override
    public void endInput() throws IOException {
        // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit
        // the
        // remaining completed files to downstream before closing the writer so that we won't miss
        // any
        // of them.
        // Note that if the task is not closed after calling endInput, checkpoint may be triggered
        // again
        // causing files to be sent repeatedly, the writer is marked as null after the last file is
        // sent
        // to guard against duplicated writes.
        flush();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("table_name", fullTableName)
                .add("subtask_id", subTaskId)
                .add("attempt_id", attemptId)
                .toString();
    }

    /** close all open files and emit files to downstream committer operator */
    private void flush() throws IOException {
        if (writer == null) {
            return;
        }

        long startNano = System.nanoTime();
        WriteResult result = writer.complete();
        writerMetrics.updateFlushResult(result);
        output.collect(new StreamRecord<>(result));
        writerMetrics.flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));

        // Set writer to null to prevent duplicate flushes in the corner case of
        // prepareSnapshotPreBarrier happening after endInput.
        writer = null;
    }
}
