package com.dtstack.flinkx.iceberg.writer;

import com.dtstack.flinkx.metrics.BaseMetric;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkXBaseMetricsWaitBarrier {
    private final IcebergOutputFormat holdOutputFormat;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkXBaseMetricsWaitBarrier.class);

    public FlinkXBaseMetricsWaitBarrier(IcebergOutputFormat holdOutputFormat) {
        Preconditions.checkNotNull(holdOutputFormat);
        this.holdOutputFormat = holdOutputFormat;
    }

    public void waitWhileMetricsInitialized() {
        int checkTime = 0;
        while (this.holdOutputFormat.getFlinkXBaseMetric() == null) {
            LOG.info(
                    "Waiting for the FlinkX base metrics initialization..., check times: {}",
                    ++checkTime);
            try {
                Thread.sleep(1000L);
            } catch (Exception ignored) {
            }
        }
    }

    public BaseMetric getMetrics() {
        return this.holdOutputFormat.getFlinkXBaseMetric();
    }
}
