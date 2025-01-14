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
package com.dtstack.flinkx.sqlservercdc;

/**
 * Date: 2019/12/03 Company: www.dtstack.com
 *
 * @author tudou
 */
public class SqlServerCdcConfigKeys {

    public static final String KEY_USER_NAME = "username";

    public static final String KEY_PASSWORD = "password";

    public static final String KEY_URL = "url";

    public static final String KEY_DATABASE_NAME = "databaseName";

    public static final String KEY_CATALOG = "cat";

    public static final String KEY_PAVING_DATA = "pavingData";

    public static final String KEY_TABLE_LIST = "tableList";

    public static final String KEY_POLL_INTERVAL = "pollInterval";

    public static final String KEY_LSN = "lsn";
}
