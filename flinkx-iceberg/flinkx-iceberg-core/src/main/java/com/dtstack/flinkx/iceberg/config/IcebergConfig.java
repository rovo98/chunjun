package com.dtstack.flinkx.iceberg.config;

import java.util.HashMap;
import java.util.Map;

public class IcebergConfig {
    private final String warehouse;
    private final String metastoreUris;
    private Map<String, Object> hadoopConfig;

    private final String database;
    private final String table;

    private IcebergConfig(Builder builder) {
        this.warehouse = builder.warehouse;
        this.metastoreUris = builder.metastoreUris;
        this.hadoopConfig = builder.hadoopConfig;
        this.database = builder.database;
        this.table = builder.table;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public String getMetastoreUris() {
        return metastoreUris;
    }

    public Map<String, Object> getHadoopConfig() {
        return hadoopConfig;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String warehouse;
        private String metastoreUris;
        private String database;
        private String table;
        private Map<String, Object> hadoopConfig = new HashMap<>();

        public Builder warehouse(String wh) {
            this.warehouse = wh;
            return this;
        }

        public Builder database(String db) {
            this.database = db;
            return this;
        }

        public Builder table(String t) {
            this.table = t;
            return this;
        }

        public Builder metastoreUris(String uris) {
            this.metastoreUris = uris;
            return this;
        }

        public Builder hadoopConfig(Map<String, Object> hdpConfig) {
            this.hadoopConfig = hdpConfig;
            return this;
        }

        public Builder putHadoopCfgEntry(String key, Object value) {
            this.hadoopConfig.put(key, value);
            return this;
        }

        public IcebergConfig build() {
            return new IcebergConfig(this);
        }
    }

    @Override
    public String toString() {
        return "IcebergConfig{"
                + "warehouse='"
                + warehouse
                + '\''
                + ", metastoreUris='"
                + metastoreUris
                + '\''
                + ", hadoopConfig="
                + hadoopConfig
                + ", database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + '}';
    }
}
