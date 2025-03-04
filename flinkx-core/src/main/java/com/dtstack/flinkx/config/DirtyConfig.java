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

package com.dtstack.flinkx.config;

import java.util.Map;

/**
 * The configuration of dirty data management
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class DirtyConfig extends AbstractConfig {

    public static final String KEY_DIRTY_PATH = "path";
    public static final String KEY_DIRTY_HADOOP_CONFIG = "hadoopConfig";

    public DirtyConfig(Map<String, Object> map) {
        super(map);
    }

    public String getPath() {
        return getStringVal(KEY_DIRTY_PATH);
    }

    public void setPath(String path) {
        setStringVal(KEY_DIRTY_PATH, path);
    }

    public Map<String, Object> getHadoopConfig() {
        return (Map<String, Object>) getVal(KEY_DIRTY_HADOOP_CONFIG);
    }

    public void setHadoopConfig(Map<String, String> hadoopConfig) {
        setVal(KEY_DIRTY_HADOOP_CONFIG, hadoopConfig);
    }
}
