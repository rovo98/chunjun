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

package com.dtstack.flinkx.es;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.TelnetUtil;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;

import java.math.BigDecimal;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities for ElasticSearch 7.x
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class Es7xUtil {

    private static final SSLContext sslContext;
    private static final HostnameVerifier hostnameVerifier;

    static {
        try {
            sslContext = SSLContext.getInstance("SSL");
            TrustManager[] trustAllCerts =
                    new TrustManager[] {
                        new X509TrustManager() {
                            @Override
                            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                                return new java.security.cert.X509Certificate[] {};
                            }

                            @Override
                            public void checkClientTrusted(
                                    X509Certificate[] chain, String authType) {}

                            @Override
                            public void checkServerTrusted(
                                    X509Certificate[] chain, String authType) {}
                        }
                    };
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        } catch (Exception e) {
            throw new ElasticsearchException(e);
        }
    }

    static {
        hostnameVerifier = (hostname, session) -> true;
    }

    public static RestHighLevelClient getClient(
            String requestSchema,
            String address,
            String username,
            String password,
            Map<String, Object> config) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = address.split(",");
        for (String add : addr) {
            String[] pair = add.split(":");
            TelnetUtil.telnet(pair[0], Integer.parseInt(pair[1]));
            httpHostList.add(new HttpHost(pair[0], Integer.parseInt(pair[1]), requestSchema));
        }
        RestClientBuilder restClientBuilder =
                RestClient.builder(httpHostList.toArray(new HttpHost[0]));

        String pathPrefix = MapUtils.getString(config, EsConfigKeys.KEY_PATH_PREFIX);
        if (StringUtils.isNotEmpty(pathPrefix)) {
            restClientBuilder.setPathPrefix(pathPrefix);
        }
        if (StringUtils.isNotBlank(username)) {

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            restClientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder ->
                            httpAsyncClientBuilder
                                    .setDefaultCredentialsProvider(credentialsProvider)
                                    .setSSLContext(sslContext)
                                    .setSSLHostnameVerifier(hostnameVerifier));
        }

        return new RestHighLevelClientBuilder(restClientBuilder.build())
                // make this client also communicate with ES 7.11 and higher.
                .setApiCompatibilityMode(false)
                .build();
    }

    public static Row jsonMapToRow(
            Map<String, Object> map, List<String> fields, List<String> types, List<String> values) {
        Preconditions.checkArgument(types.size() == fields.size());
        Row row = new Row(fields.size());

        for (int i = 0; i < fields.size(); ++i) {
            String field = fields.get(i);
            if (StringUtils.isNotBlank(field)) {
                String[] parts = field.split("\\.");
                Object value = readMapValue(map, parts);
                row.setField(i, value);
            } else {
                Object value = convertValueToAssignType(types.get(i), values.get(i));
                row.setField(i, value);
            }
        }

        return row;
    }

    public static Map<String, Object> rowToJsonMap(Row row, List<String> fields, List<String> types)
            throws WriteRecordException {
        Preconditions.checkArgument(row.getArity() == fields.size());
        Map<String, Object> jsonMap = new HashMap<>((fields.size() << 2) / 3);
        int i = 0;
        try {
            for (; i < fields.size(); ++i) {
                String field = fields.get(i);
                String[] parts = field.split("\\.");
                Map<String, Object> currMap = jsonMap;
                for (int j = 0; j < parts.length - 1; ++j) {
                    String key = parts[j];
                    if (currMap.get(key) == null) {
                        currMap.put(key, new HashMap<String, Object>(16));
                    }
                    currMap = (Map<String, Object>) currMap.get(key);
                }
                String key = parts[parts.length - 1];
                Object col = row.getField(i);
                if (col != null) {
                    col = StringUtil.string2col(String.valueOf(col), types.get(i), null);
                }

                currMap.put(key, col);
            }
        } catch (Exception ex) {
            String msg =
                    "EsUtil.rowToJsonMap Writing record error: when converting field["
                            + i
                            + "] in Row("
                            + row
                            + ")";
            throw new WriteRecordException(msg, ex, i, row);
        }

        return jsonMap;
    }

    private static Object readMapValue(Map<String, Object> jsonMap, String[] fieldParts) {
        Map<String, Object> current = jsonMap;
        int i = 0;
        for (; i < fieldParts.length - 1; ++i) {
            if (current.containsKey(fieldParts[i])) {
                current = (Map<String, Object>) current.get(fieldParts[i]);
            } else {
                return null;
            }
        }
        return current.get(fieldParts[i]);
    }

    private static Object convertValueToAssignType(String columnType, String constantValue) {
        Object column = null;
        if (StringUtils.isEmpty(constantValue)) {
            return column;
        }

        switch (columnType.toUpperCase()) {
            case "BOOLEAN":
                column = Boolean.valueOf(constantValue);
                break;
            case "SHORT":
            case "INT":
            case "LONG":
                column = NumberUtils.createBigDecimal(constantValue).toBigInteger();
                break;
            case "FLOAT":
            case "DOUBLE":
                column = new BigDecimal(constantValue);
                break;
            case "STRING":
                column = constantValue;
                break;
            case "DATE":
                column = DateUtil.stringToDate(constantValue, null);
                break;
            default:
                throw new IllegalArgumentException("Unsupported column type: " + columnType);
        }
        return column;
    }

    public static String[] getStringArray(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            String stringValue = value.toString();
            return stringValue.split(",");
        } else if (value instanceof List) {
            List list = (List) value;
            String[] array = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                array[i] = list.get(i).toString();
            }

            return array;
        } else {
            return new String[] {value.toString()};
        }
    }
}
