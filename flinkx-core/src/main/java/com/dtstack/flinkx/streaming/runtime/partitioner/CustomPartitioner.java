/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

/**
 * Rewrite the [DTRebalancePartitioner] to distribute data based on the channel specified in the
 * data
 *
 * @param <T> Type of the elements in the Stream being rebalanced
 * @author jiangbo TODO 这个类后面会删掉
 */
@Internal
public class CustomPartitioner<T> extends StreamPartitioner<T> {
    private static final long serialVersionUID = 1L;

    @Override
    public int selectChannel(
            SerializationDelegate<StreamRecord<T>> streamRecordSerializationDelegate) {
        Row row = (Row) streamRecordSerializationDelegate.getInstance().getValue();
        return (Integer) row.getField(row.getArity() - 1);
    }

    @Override
    public StreamPartitioner<T> copy() {
        return this;
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.FULL;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "REBALANCE";
    }
}
