package com.dtstack.flinkx.iceberg.config;

public class IcebergConfigKeys {

    /** specify the iceberg (hive catalog impl) warehouse location */
    public static final String KEY_WAREHOUSE = "warehouse";

    /** Hive metastore uris */
    public static final String KEY_METASTORE_URIS = "uri";

    /** Specify customized hadoop configurations */
    public static final String KEY_HADOOP_CONFIG = "hadoopConfig";

    /** Specify the database in the catalog. */
    public static final String KEY_DATABASE = "database";

    /** Specify the table in the catalog. */
    public static final String KEY_TABLE = "table";

    /** Specify data write mode for writer. */
    public static final String KEY_WRITE_MODE = "writeMode";

    public static final String KEY_COLUMN_NAME = "name";

    public static final String KEY_COLUMN_TYPE = "type";
}
