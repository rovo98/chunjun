package com.dtstack.flinkx.starrocks;

import com.dtstack.flinkx.enums.ColumnType;

import com.google.common.base.Preconditions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public final class StarRocksUtil {
    private StarRocksUtil() {}

    private static String starRocksType2FlinkXType(String type) {
        switch (type.toLowerCase()) {
            case "integer":
                return "int";
            default:
                return type;
        }
    }

    public static DataType internalType2FlinkDataType(final String type) {
        ColumnType columnType = ColumnType.getType(starRocksType2FlinkXType(type));
        switch (columnType) {
            case TINYINT:
                return DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INT:
                return DataTypes.INT();
            case MEDIUMINT:
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case STRING:
            case CHAR:
            case VARCHAR:
                return DataTypes.STRING();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case DATE:
                return DataTypes.DATE();
            case TIME:
                return DataTypes.TIME();
            case TIMESTAMP:
            case DATETIME:
                return DataTypes.TIMESTAMP(9);
            case DECIMAL:
                int startPIdx = type.indexOf("(");
                int endPIdx = type.indexOf(")");
                if (startPIdx < 0 || endPIdx < 0) {
                    // TBD: to be handle later.
                    return null;
                }
                Preconditions.checkState(
                        startPIdx > 0 && endPIdx > 0, "Decimal type require precision and scale.");
                String[] ps = type.substring(startPIdx + 1, endPIdx).split(",");
                int precision = Integer.parseInt(ps[0].trim());
                int scale = Integer.parseInt(ps[1].trim());
                return DataTypes.DECIMAL(precision, scale);
            default:
                throw new UnsupportedOperationException("Unsupported type -> `" + type + "`.");
        }
    }

    public static DataType toFlinkDecimalType(String type) {
        int startPIdx = type.indexOf("(");
        int endPIdx = type.indexOf(")");
        Preconditions.checkState(
                startPIdx > 0 && endPIdx > 0, "Decimal type require precision and scale.");
        String[] ps = type.substring(startPIdx + 1, endPIdx).split(",");
        int precision = Integer.parseInt(ps[0].trim());
        int scale = Integer.parseInt(ps[1].trim());
        return DataTypes.DECIMAL(precision, scale);
    }
}
