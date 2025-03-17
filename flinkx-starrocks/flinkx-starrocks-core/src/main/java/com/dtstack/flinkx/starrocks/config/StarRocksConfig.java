package com.dtstack.flinkx.starrocks.config;

import java.util.Map;

/** configuration properties for connector-starrocks. */
public class StarRocksConfig {
    // required
    private final String jdbcUrl;
    private final String httpUrl;
    private final String database;
    private final String table;
    private final String username;
    private final String password;

    // optional
    private final Map<String, String> optionalProps;

    private StarRocksConfig(Builder builder) {
        this.jdbcUrl = builder.jdbcUrl;
        this.httpUrl = builder.httpUrl;
        this.database = builder.database;
        this.table = builder.table;
        this.username = builder.username;
        this.password = builder.password;
        this.optionalProps = builder.optionalProps;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getHttpUrl() {
        return httpUrl;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Map<String, String> getOptionalProps() {
        return optionalProps;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String jdbcUrl;
        private String httpUrl;
        private String database;
        private String table;
        private String username;
        private String password;
        private Map<String, String> optionalProps;

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder httpUrl(String loadUrl) {
            this.httpUrl = loadUrl;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder optionalProps(Map<String, String> props) {
            this.optionalProps = props;
            return this;
        }

        public StarRocksConfig build() {
            return new StarRocksConfig(this);
        }
    }

    @Override
    public String toString() {
        return "StarRocksConfig{"
                + "jdbcUrl='"
                + jdbcUrl
                + '\''
                + ", httpUrl='"
                + httpUrl
                + '\''
                + ", database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", username='"
                + username
                + '\''
                + ", password='******'"
                + ", optionalProps="
                + optionalProps
                + '}';
    }
}
