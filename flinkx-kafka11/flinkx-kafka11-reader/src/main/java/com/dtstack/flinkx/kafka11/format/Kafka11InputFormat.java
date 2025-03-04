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

package com.dtstack.flinkx.kafka11.format;

import com.dtstack.flinkx.kafka11.client.Kafka11Consumer;
import com.dtstack.flinkx.kafkabase.enums.KafkaVersion;
import com.dtstack.flinkx.kafkabase.format.KafkaBaseInputFormat;
import com.dtstack.flinkx.kafkabase.util.KafkaUtil;

import java.io.IOException;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/5
 */
public class Kafka11InputFormat extends KafkaBaseInputFormat {

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        Properties props = KafkaUtil.geneConsumerProp(consumerSettings, mode);
        consumer = new Kafka11Consumer(props);
    }

    @Override
    public KafkaVersion getKafkaVersion() {
        return KafkaVersion.kafka11;
    }
}
