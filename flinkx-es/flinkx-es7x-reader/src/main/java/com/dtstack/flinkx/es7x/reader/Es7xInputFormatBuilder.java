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

package com.dtstack.flinkx.es7x.reader;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;

import java.util.List;
import java.util.Map;

/**
 * The builder class of EsInputFormat
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class Es7xInputFormatBuilder extends BaseRichInputFormatBuilder {

    private Es7xInputFormat format;

    public Es7xInputFormatBuilder() {
        super.format = format = new Es7xInputFormat();
    }

    public Es7xInputFormatBuilder setAddress(String address) {
        format.address = address;
        return this;
    }

    public Es7xInputFormatBuilder setUsername(String username) {
        format.username = username;
        return this;
    }

    public Es7xInputFormatBuilder setPassword(String password) {
        format.password = password;
        return this;
    }

    public Es7xInputFormatBuilder setQuery(String query) {
        format.query = query;
        return this;
    }

    public Es7xInputFormatBuilder setColumnNames(String query) {
        format.query = query;
        return this;
    }

    public Es7xInputFormatBuilder setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
        return this;
    }

    public Es7xInputFormatBuilder setColumnValues(List<String> columnValues) {
        format.columnValues = columnValues;
        return this;
    }

    public Es7xInputFormatBuilder setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
        return this;
    }

    public Es7xInputFormatBuilder setIndex(String[] index) {
        format.index = index;
        return this;
    }

    public Es7xInputFormatBuilder setType(String[] type) {
        format.type = type;
        return this;
    }

    public Es7xInputFormatBuilder setBatchSize(Integer batchSize) {
        if (batchSize != null && batchSize > 0) {
            format.batchSize = batchSize;
        }
        return this;
    }

    public Es7xInputFormatBuilder setClientConfig(Map<String, Object> clientConfig) {
        format.clientConfig = clientConfig;
        return this;
    }

    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()) {
            throw new UnsupportedOperationException(
                    "This plugin not support restore from failed state");
        }

        if (format.batchSize > ConstantValue.MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("批量读取数量不能大于[200000]条");
        }
    }
}
