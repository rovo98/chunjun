/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metrics;

import com.dtstack.flinkx.util.ReflectionUtils;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Regularly get statistics from the flink API
 *
 * @author jiangbo
 * @date 2019/7/17
 */
public class AccumulatorCollector {

    private static final Logger LOG = LoggerFactory.getLogger(AccumulatorCollector.class);
    private static final String THREAD_NAME = "accumulator-collector-thread";
    private static final int MAX_COLLECT_ERROR_TIMES = 100;
    private final long period;

    private JobMasterGateway gateway;
    private final ScheduledExecutorService scheduledExecutorService;

    private final Map<String, ValueAccumulator> valueAccumulatorMap;
    private long collectErrorTimes = 0;

    public AccumulatorCollector(StreamingRuntimeContext context, List<String> metricNames) {
        Preconditions.checkArgument(metricNames != null && metricNames.size() > 0);
        // initialize valueAccumulatorMap
        valueAccumulatorMap = new HashMap<>(metricNames.size());
        for (String metricName : metricNames) {
            valueAccumulatorMap.put(
                    metricName, new ValueAccumulator(0, context.getLongCounter(metricName)));
        }

        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(1, r -> new Thread(r, THREAD_NAME));

        // 比 task manager 心跳间隔多 1 秒
        this.period =
                ((TaskManagerConfiguration) context.getTaskManagerRuntimeInfo())
                                .getTimeout()
                                .toMilliseconds()
                        + 1000;

        RpcGlobalAggregateManager globalAggregateManager =
                ((RpcGlobalAggregateManager) context.getGlobalAggregateManager());
        Field field = ReflectionUtils.getDeclaredField(globalAggregateManager, "jobMasterGateway");
        assert field != null;
        field.setAccessible(true);
        try {
            gateway = (JobMasterGateway) field.get(globalAggregateManager);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    public void start() {
        scheduledExecutorService.scheduleAtFixedRate(
                this::collectAccumulator, 0, period, TimeUnit.MILLISECONDS);
    }

    public void close() {
        if (scheduledExecutorService != null
                && !scheduledExecutorService.isShutdown()
                && !scheduledExecutorService.isTerminated()) {
            scheduledExecutorService.shutdown();
            LOG.info("AccumulatorCollector#scheduledExecutorService shutdown.");
        }
    }

    public void collectAccumulator() {
        try {
            if (gateway != null) {
                CompletableFuture<ArchivedExecutionGraph> archivedExecutionGraphFuture =
                        gateway.requestJob(Time.seconds(10));
                ArchivedExecutionGraph archivedExecutionGraph = archivedExecutionGraphFuture.get();

                // update value accumulators.
                StringifiedAccumulatorResult[] accumulatorResult =
                        archivedExecutionGraph.getAccumulatorResultsStringified();
                for (StringifiedAccumulatorResult result : accumulatorResult) {
                    LOG.info(
                            "Queried accumulator name -> {}, value -> {}.",
                            result.getName(),
                            result.getValue());
                    ValueAccumulator valueAccumulator = valueAccumulatorMap.get(result.getName());
                    if (valueAccumulator != null) {
                        valueAccumulator.setGlobal(Long.parseLong(result.getValue()));
                    }
                }
            } else {
                throw new RuntimeException("The jobMasterGateway is uninitialized!");
            }
        } catch (Exception e) {
            // 限制最大出错次数，超过最大次数则使任务失败，如果不失败，统计数据没有及时更新，会影响速率控制，错误控制等功能
            collectErrorTimes++;
            if (collectErrorTimes > MAX_COLLECT_ERROR_TIMES) {
                // 主动关闭线程和资源，避免异常情况下没有关闭
                throw new RuntimeException(
                        "The number of errors in updating statistics data exceeds the maximum limit of 100 times."
                                + " To ensure the correctness of the data, the task automatically fails");
            }
        }
    }

    public long getAccumulatorValue(String name) {
        ValueAccumulator valueAccumulator = valueAccumulatorMap.get(name);
        if (valueAccumulator == null) {
            return 0;
        }
        return valueAccumulator.getGlobal();
    }

    public long getLocalAccumulatorValue(String name) {
        ValueAccumulator valueAccumulator = valueAccumulatorMap.get(name);
        if (valueAccumulator == null) {
            return 0;
        }

        return valueAccumulator.getLocal().getLocalValue();
    }

    static class ValueAccumulator {
        private long global;
        private LongCounter local;

        public ValueAccumulator(long global, LongCounter local) {
            this.global = global;
            this.local = local;
        }

        public long getGlobal() {
            return global;
        }

        public LongCounter getLocal() {
            return local;
        }

        public void setGlobal(long global) {
            this.global = global;
        }
    }
}
