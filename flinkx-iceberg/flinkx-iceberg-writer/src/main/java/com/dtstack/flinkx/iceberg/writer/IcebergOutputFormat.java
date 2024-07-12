package com.dtstack.flinkx.iceberg.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;

import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// NOTE: useless anymore. Remove it in the future.
public class IcebergOutputFormat extends BaseRichOutputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergOutputFormat.class);

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {}

    @Override
    protected void writeSingleRecord(Row row) {
        // NOTE: do nothing there.
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        // NOTE: do nothing here.
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        notSupportBatchWrite("IcebergWriter");
    }
}
