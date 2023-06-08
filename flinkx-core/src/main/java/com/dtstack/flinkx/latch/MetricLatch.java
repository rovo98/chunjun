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

package com.dtstack.flinkx.latch;

import com.dtstack.flinkx.util.ReflectionUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed implementation of Latch
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class MetricLatch extends BaseLatch {

    public static Logger LOG = LoggerFactory.getLogger(MetricLatch.class);

    private final String metricName;
    private final StreamingRuntimeContext context;
    private JobMasterGateway gateway;
    private static final String METRIC_PREFIX = "latch-";

    public MetricLatch(RuntimeContext context, String metricName) {
        this.metricName = METRIC_PREFIX + metricName;
        this.context = (StreamingRuntimeContext) context;

        RpcGlobalAggregateManager globalAggregateManager =
                ((RpcGlobalAggregateManager)
                        ((StreamingRuntimeContext) context).getGlobalAggregateManager());
        Field field = ReflectionUtils.getDeclaredField(globalAggregateManager, "jobMasterGateway");
        assert field != null;
        field.setAccessible(true);
        try {
            this.gateway = (JobMasterGateway) field.get(globalAggregateManager);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    @Override
    public int getVal() {
        try {
            if (gateway != null) {
                CompletableFuture<ExecutionGraphInfo> executionGraphInfoFuture =
                        gateway.requestJob(Time.seconds(10));
                ExecutionGraphInfo executionGraphInfo = executionGraphInfoFuture.get();
                // update value accumulators.
                StringifiedAccumulatorResult[] accumulatorResult =
                        executionGraphInfo
                                .getArchivedExecutionGraph()
                                .getAccumulatorResultsStringified();
                for (StringifiedAccumulatorResult result : accumulatorResult) {
                    LOG.info(
                            "Queried accumulator name -> {}, value -> {}.",
                            result.getName(),
                            result.getValue());
                    if (metricName != null && metricName.equals(result.getName())) {
                        return Integer.parseInt(result.getValue());
                    }
                }
            } else {
                throw new RuntimeException("The jobMasterGateway is uninitialized!");
            }
        } catch (Exception e) {
            LOG.error("failed to request archived execution graph.", e);
        }
        return -1;
    }

    @Override
    public void addOne() {
        context.getIntCounter(metricName).add(1);
    }
}
