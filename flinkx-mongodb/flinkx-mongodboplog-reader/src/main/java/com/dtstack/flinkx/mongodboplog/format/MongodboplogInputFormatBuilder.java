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

package com.dtstack.flinkx.mongodboplog.format;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.mongodb.MongodbConfig;

/**
 * @author jiangbo
 * @date 2019/12/5
 */
public class MongodboplogInputFormatBuilder extends BaseRichInputFormatBuilder {

    private MongodboplogInputFormat format;

    public MongodboplogInputFormatBuilder() {
        super.format = this.format = new MongodboplogInputFormat();
    }

    public void setMongodbConfig(MongodbConfig mongodbConfig) {
        format.mongodbConfig = mongodbConfig;
    }

    @Override
    protected void checkFormat() {}
}
