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


package com.dtstack.flinkx.parser;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.net.URL;
import java.util.List;
import java.util.regex.Pattern;

/**
 * parser create tmp table sql
 * Date: 2018/6/26
 * Company: www.dtstack.com
 *
 * @author yanxi
 */
public class CreateTmpTableParser implements IParser {

    //select table tableName as select
    private static final String TEMPORARY_STR = "(?i)create\\s+(temporary\\s+)?view\\s+(if\\s+not\\s+exists\\s+)?([^\\s]+)\\s+as\\s+select\\s+(.*)";

    private static final String EMPTY_STR = "(?i)^\\screate\\s+view\\s+(\\S+)\\s*\\((.+)\\)$";

    private static final Pattern TEMPORARYVIEW = Pattern.compile(TEMPORARY_STR);

    public static CreateTmpTableParser newInstance() {
        return new CreateTmpTableParser();
    }

    @Override
    public boolean verify(String sql) {
        if (Pattern.compile(EMPTY_STR).matcher(sql).find()) {
            return true;
        }
        return TEMPORARYVIEW.matcher(sql).find();
    }

    @Override
    public void execSql(String sql, StreamTableEnvironment tableEnvironment, StatementSet statementSet, List<URL> jarUrlList) {
        tableEnvironment.executeSql(sql);
    }
}