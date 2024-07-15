package com.dtstack.flinkx.iceberg.writer;

public enum WriteMode {
    DEFAULT,
    UPSERT,
    OVERWRITE;

    public static WriteMode of(String wm) {
        for (WriteMode writeMode : WriteMode.values()) {
            if (wm.equalsIgnoreCase(writeMode.name())) {
                return writeMode;
            }
        }
        return DEFAULT;
    }
}
