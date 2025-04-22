package com.dtstack.flinkx.starrocks;

import com.dtstack.flinkx.enums.ColumnType;

import com.google.common.base.Preconditions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Optional;

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
        Optional<DataType> dtOpt;
        switch (columnType) {
            case TINYINT:
                dtOpt = TypeConversions.fromClassToDataType(Byte.class);
                break;
            case SMALLINT:
                dtOpt = TypeConversions.fromClassToDataType(Short.class);
                break;
            case INT:
                dtOpt = TypeConversions.fromClassToDataType(Integer.class);
                break;
            case MEDIUMINT:
            case BIGINT:
                dtOpt = TypeConversions.fromClassToDataType(Long.class);
                break;
            case FLOAT:
                dtOpt = TypeConversions.fromClassToDataType(Float.class);
                break;
            case DOUBLE:
                dtOpt = TypeConversions.fromClassToDataType(Double.class);
                break;
            case STRING:
            case CHAR:
            case VARCHAR:
                dtOpt = TypeConversions.fromClassToDataType(String.class);
                break;
            case BOOLEAN:
                dtOpt = TypeConversions.fromClassToDataType(Boolean.class);
                break;
            case DATE:
                dtOpt = TypeConversions.fromClassToDataType(Date.class);
                break;
            case TIME:
                dtOpt = TypeConversions.fromClassToDataType(Time.class);
                break;
            case TIMESTAMP:
            case DATETIME:
                dtOpt = TypeConversions.fromClassToDataType(Timestamp.class);
                break;
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
                DecimalType flinkDt = new DecimalType(precision, scale);
                dtOpt = Optional.ofNullable(TypeConversions.fromLogicalToDataType(flinkDt));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type -> `" + type + "`.");
        }
        return dtOpt.orElseThrow(
                () ->
                        new UnsupportedOperationException(
                                "Failed to convert type `" + type + "` into Flink datatype."));
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
