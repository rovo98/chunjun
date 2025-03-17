package com.starrocks.connector.flink;

import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.RetryUtil;

import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionIProvider;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

public class StarRocksOpsHook implements OperationHook {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksOpsHook.class);

    private int taskNum;

    private List<String> preSql;
    private List<String> postSql;

    private StarRocksJdbcConnectionIProvider jdbcConnProvider;

    public StarRocksOpsHook(
            int taskNum,
            List<String> preSql,
            List<String> postSql,
            StarRocksJdbcConnectionOptions jdbcConnectionOptions) {
        this.taskNum = taskNum;
        this.preSql = preSql;
        this.postSql = postSql;
        this.jdbcConnProvider = new StarRocksJdbcConnectionProvider(jdbcConnectionOptions);
    }

    @Override
    public boolean needWaitBeforeWriteRecords() {
        return CollectionUtils.isNotEmpty(preSql);
    }

    @Override
    public void beforeWriteRecords() {
        if (taskNum == 0) {
            LOG.info("Try to perform preSql:>`{}`.", preSql);
            RetryUtil.executeWithRetry(
                    () -> {
                        Connection conn = jdbcConnProvider.reestablishConnection();
                        try {
                            DbUtil.executeBatch(conn, preSql);
                            LOG.info("preSQLs executed successfully.");
                        } catch (Exception e) {
                            LOG.error(
                                    "Failed to execute pre SQLs - {}, err msg -> {}.",
                                    preSql,
                                    e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    },
                    3,
                    2000,
                    true);
        }
    }

    @Override
    public boolean needWaitBeforeClose() {
        return CollectionUtils.isNotEmpty(postSql);
    }

    @Override
    public void beforeClose() {
        if (taskNum == 0) {
            LOG.info("Try to perform postSql:>`{}`.", postSql);
            RetryUtil.executeWithRetry(
                    () -> {
                        Connection conn = jdbcConnProvider.reestablishConnection();
                        try {
                            DbUtil.executeBatch(conn, postSql);
                            LOG.info("postSQLs executed successfully!");
                        } catch (Exception e) {
                            LOG.error(
                                    "Failed to execute post SQLs - {}, err msg -> {}.",
                                    postSql,
                                    e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    },
                    3,
                    2000,
                    true);
        }
    }
}
