/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.kudu.core;

/**
 * @author jiangbo
 * @date 2019/8/12
 */
public class KuduConfigKeys {

    public static final String KEY_MASTER_ADDRESSES = "masterAddresses";
    public static final String KEY_AUTHENTICATION = "authentication";
    public static final String KEY_PRINCIPAL = "principal";
    public static final String KEY_KEYTABFILE = "keytabFile";
    public static final String KEY_WORKER_COUNT = "workerCount";
    public static final String KEY_BOSS_COUNT = "bossCount";
    public static final String KEY_OPERATION_TIMEOUT = "operationTimeout";
    public static final String KEY_QUERY_TIMEOUT = "queryTimeout";
    public static final String KEY_ADMIN_OPERATION_TIMEOUT = "adminOperationTimeout";
    public static final String KEY_TABLE = "table";
    public static final String KEY_READ_MODE = "readMode";
    public static final String KEY_FLUSH_MODE = "flushMode";
    public static final String KEY_FILTER = "where";
    public static final String KEY_BATCH_SIZE_BYTES = "batchSizeBytes";
}
