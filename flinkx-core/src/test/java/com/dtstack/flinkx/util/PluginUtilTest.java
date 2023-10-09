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

package com.dtstack.flinkx.util;

import com.dtstack.flinkx.classloader.PluginUtil;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

/** @author tiezhu Date 2020/6/19 星期五 */
public class PluginUtilTest {

    @Test
    public void testGetJarFileDirPath() {
        String pluginName = "mysqlreader";
        String pluginRoot =
                "F:\\dtstack_workplace\\project_workplace\\flinkx\\code\\flinkx\\syncplugins";
        String remotePluginPath =
                "F:\\dtstack_workplace\\project_workplace\\flinkx\\code\\flinkx\\syncplugins";

        assertEquals(
                4, PluginUtil.getJarFileDirPath(pluginName, pluginRoot, remotePluginPath).size());
    }

    private static final String PKG_PREFIX = "com.dtstack.flinkx.";

    @Test
    public void testCamelize()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method camelizeMethod =
                PluginUtil.class.getDeclaredMethod("camelize", String.class, String.class);
        camelizeMethod.setAccessible(true);

        String es6xReaderFullClassName =
                PKG_PREFIX.concat((String) camelizeMethod.invoke(null, "es6xreader", "reader"));
        String es6xWriterFullClassName =
                PKG_PREFIX.concat((String) camelizeMethod.invoke(null, "es6xwriter", "writer"));

        String es7xReaderFullClassName =
                PKG_PREFIX.concat((String) camelizeMethod.invoke(null, "es7xreader", "reader"));
        String es7xWriterFullClassName =
                PKG_PREFIX.concat((String) camelizeMethod.invoke(null, "es7xwriter", "writer"));

        String expectedEs6xReaderCn = "com.dtstack.flinkx.es6x.reader.Es6xReader";
        String expectedEs6xWriterCn = "com.dtstack.flinkx.es6x.writer.Es6xWriter";
        String expectedEs7xReaderCn = "com.dtstack.flinkx.es7x.reader.Es7xReader";
        String expectedEs7xWriterCn = "com.dtstack.flinkx.es7x.writer.Es7xWriter";

        assertEquals(expectedEs6xReaderCn, es6xReaderFullClassName);
        assertEquals(expectedEs6xWriterCn, es6xWriterFullClassName);

        assertEquals(expectedEs7xReaderCn, es7xReaderFullClassName);
        assertEquals(expectedEs7xWriterCn, es7xWriterFullClassName);
    }
}
