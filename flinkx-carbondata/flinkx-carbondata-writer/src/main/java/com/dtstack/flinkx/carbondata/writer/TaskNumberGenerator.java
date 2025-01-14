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

package com.dtstack.flinkx.carbondata.writer;

/**
 * task number generator
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan_zju@163.com
 */
public class TaskNumberGenerator {

    private TaskNumberGenerator() {}

    /** Generate unique number to be used as partition number of file name */
    public static String generateUniqueNumber(int taskId, String segmentId, long partitionNumber) {
        return String.valueOf((long) (Math.pow(10, 2)) + Integer.valueOf(segmentId))
                + String.valueOf((long) Math.pow(10, 5) + taskId)
                + String.valueOf(partitionNumber + (long) Math.pow(10, 5));
    }
}
