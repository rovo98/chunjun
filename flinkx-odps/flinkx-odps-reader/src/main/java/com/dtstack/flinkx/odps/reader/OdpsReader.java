/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.odps.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.odps.OdpsConfigKeys.KEY_ODPS_CONFIG;
import static com.dtstack.flinkx.odps.OdpsConfigKeys.KEY_PARTITION;
import static com.dtstack.flinkx.odps.OdpsConfigKeys.KEY_TABLE;

/**
 * The reader plugin of Odps
 *
 * @author huyifan.zju@163.com
 * @date 2018-1-17
 */
public class OdpsReader extends BaseDataReader {
    private Map<String, String> odpsConfig;
    private List<MetaColumn> metaColumns;

    protected String tableName;
    protected String partition;

    public OdpsReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        odpsConfig = (Map<String, String>) readerConfig.getParameter().getVal(KEY_ODPS_CONFIG);
        tableName = readerConfig.getParameter().getStringVal(KEY_TABLE);
        partition = readerConfig.getParameter().getStringVal(KEY_PARTITION);

        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());
    }

    @Override
    public DataStream<Row> readData() {
        OdpsInputFormatBuilder builder = new OdpsInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setMetaColumn(metaColumns);
        builder.setOdpsConfig(odpsConfig);
        builder.setTableName(tableName);
        builder.setPartition(partition);
        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);

        return createInput(builder.finish());
    }
}
