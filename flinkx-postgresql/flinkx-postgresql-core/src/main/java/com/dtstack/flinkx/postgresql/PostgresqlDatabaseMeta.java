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

package com.dtstack.flinkx.postgresql;

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The class of PostgreSQL database prototype @Company: www.dtstack.com
 *
 * @author jiangbo
 */
public class PostgresqlDatabaseMeta extends BaseDatabaseMeta {

    @Override
    protected String makeValues(List<String> column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStartQuote() {
        return "\"";
    }

    @Override
    public String getEndQuote() {
        return "\"";
    }

    @Override
    public String quoteValue(String value, String column) {
        return String.format("'%s' as %s", value, column);
    }

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.PostgreSQL;
    }

    @Override
    public String getDriverClass() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return String.format("SELECT * FROM %s LIMIT 0", tableName);
    }

    @Override
    public String getSqlQueryColumnFields(List<String> column, String table) {
        String sql =
                "select attrelid ::regclass as table_name, attname as col_name, atttypid ::regtype as col_type from pg_attribute \n"
                        + "where attrelid = '%s' ::regclass and attnum > 0 and attisdropped = 'f'";
        return String.format(sql, table);
    }

    @Override
    public String getUpsertStatement(
            List<String> column, String table, Map<String, List<String>> updateKey) {
        if (updateKey == null || updateKey.isEmpty()) {
            return getInsertStatement(column, table);
        }
        updateKey.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        if (updateKey.isEmpty()) {
            return getInsertStatement(column, table);
        }
        return "INSERT INTO "
                + quoteTable(table)
                + " ("
                + quoteColumns(column)
                + ") values "
                + makeValues(column.size())
                + " ON CONFLICT ("
                + updateKeySql(updateKey)
                + ") DO UPDATE SET "
                + makeUpdatePart(
                        column,
                        updateKey.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toSet()));
    }

    protected String updateKeySql(Map<String, List<String>> updateKey) {
        List<String> allUpdateKeys = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : updateKey.entrySet()) {
            allUpdateKeys.addAll(entry.getValue());
        }
        return StringUtils.join(allUpdateKeys, ",");
    }

    private String makeUpdatePart(List<String> column, Set<String> uniqueKeys) {
        List<String> updateList = new ArrayList<>();
        for (String col : column) {
            if (!uniqueKeys.contains(col)) {
                String quotedCol = quoteColumn(col);
                updateList.add(quotedCol + "=excluded." + quotedCol);
            }
        }
        return StringUtils.join(updateList, ",");
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format(" mod(%s,${N}) = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getSplitFilterWithTmpTable(String tmpTable, String columnName) {
        return String.format(
                " mod(%s.%s,${N}) = ${M}", tmpTable, getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public int getFetchSize() {
        return 1000;
    }

    @Override
    public int getQueryTimeout() {
        return 1000;
    }

    private String makeValues(int nCols) {
        return "(" + StringUtils.repeat("?", ",", nCols) + ")";
    }
}
