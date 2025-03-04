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

package com.dtstack.flinkx.mysql.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.mysql.MySqlDatabaseMeta;
import com.dtstack.flinkx.mysql.format.MysqlInputFormat;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import com.dtstack.flinkx.rdb.util.DbUtil;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * MySQL reader plugin
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class MysqlReader extends JdbcDataReader {

    public MysqlReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new MySqlDatabaseMeta());
        dbUrl =
                DbUtil.formatJdbcUrl(
                        dbUrl, Collections.singletonMap("zeroDateTimeBehavior", "convertToNull"));
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new MysqlInputFormat());
    }
}
