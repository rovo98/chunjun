package com.starrocks.connector.flink;

import com.dtstack.flinkx.latch.BaseLatch;
import com.dtstack.flinkx.latch.MetricLatch;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

public class SinkHookOpsManager implements Serializable {
    private static final long serialVersionUID = 1L;

    private OperationHook operationHook;

    private RuntimeContext runtimeContext;

    private int numTasks;

    public SinkHookOpsManager(OperationHook operationHook, RuntimeContext runtimeContext) {
        this.operationHook = operationHook;
        this.runtimeContext = runtimeContext;
        this.numTasks = this.runtimeContext.getNumberOfParallelSubtasks();
    }

    public void opsRunBeforeWrite() {
        if (operationHook.needWaitBeforeWriteRecords()) {
            operationHook.beforeWriteRecords();
            waitWhile("#2");
        }
    }

    public void opsRunBeforeClose() {
        if (operationHook.needWaitBeforeClose()) {
            operationHook.beforeClose();
            waitWhile("#3");
        }
    }

    private void waitWhile(String latchName) {
        BaseLatch latch = new MetricLatch(runtimeContext, latchName);
        latch.addOne();
        latch.waitUntil(numTasks);
    }
}
