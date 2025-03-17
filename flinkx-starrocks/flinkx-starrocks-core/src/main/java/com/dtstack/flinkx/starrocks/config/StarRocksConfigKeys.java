package com.dtstack.flinkx.starrocks.config;

public final class StarRocksConfigKeys {

    /**
     * comma-separated mysql jdbc url format, to connect MySQL servers on the FE nodes. <br>
     * e.g. jdbc:mysql://fe_host1:fe_query_port1,fe_host2:fe_query_port2
     */
    public static final String KEY_JDBC_URL = "jdbc-url";

    /**
     * comma-separated url format, to connect HTTP servers on the FE nodes. <br>
     * e.g. fe_host1:fe_http_port1,fe_host2:fe_http_port2
     */
    public static final String KEY_HTTP_URL = "http-url";

    public static final String KEY_DATABASE = "database";

    public static final String KEY_TABLE = "table";

    public static final String KEY_USERNAME = "username";

    public static final String KEY_PASSWORD = "password";

    public static final String KEY_OPTION_PROPS = "optional-props";
}
