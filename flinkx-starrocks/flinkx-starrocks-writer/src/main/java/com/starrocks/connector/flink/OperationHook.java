package com.starrocks.connector.flink;

/** Define operations executed during different phases of sink function */
public interface OperationHook {

    boolean needWaitBeforeWriteRecords();

    void beforeWriteRecords();

    boolean needWaitBeforeClose();

    void beforeClose();
}
