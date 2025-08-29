package com.dtstack.flinkx.starrocks;

import com.dtstack.flinkx.enums.ColumnType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Set;

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

    public static String backtickPossibleReservedKeyword(String columnName) {
        final String cn = columnName.trim();
        if (RESERVED_KEYWORDS.contains(cn.toUpperCase())) {
            return String.format("`%s`", cn);
        }
        // do nothing
        return cn;
    }

    public static Set<String> RESERVED_KEYWORDS =
            Sets.newHashSet(
                    // A
                    "ADD",
                    "ALL",
                    "ALTER",
                    "ANALYZE",
                    "AND",
                    "ARRAY",
                    "AS",
                    "ASC",
                    // B
                    "BETWEEN",
                    "BIGINT",
                    "BITMAP",
                    "BOTH",
                    "BY",
                    // C
                    "CASE",
                    "CHAR",
                    "CHARACTER",
                    "CHECK",
                    "COLUMN",
                    "COMPACTION",
                    "CONVERT",
                    "CREATE",
                    "CROSS",
                    "CUBE",
                    "CURRENT_DATE",
                    "CURRENT_TIME",
                    "CURRENT_TIMESTAMP",
                    "CURRENT_USER",
                    "CURRENT_ROLE",
                    // D
                    "DATABASE",
                    "DATABASES",
                    "DECIMAL",
                    "DECIMALV2",
                    "DECIMAL32",
                    "DECIMAL64",
                    "DEFAULT",
                    "DELETE",
                    "DENSE_RANK",
                    "DESC",
                    "DESCRIBE",
                    "DISTINCT",
                    "DOUBLE",
                    "DROP",
                    "DUAL",
                    "DEFERRED",
                    // E
                    "ELSE",
                    "EXCEPT",
                    "EXISTS",
                    "EXPLAIN",
                    // F
                    "FALSE",
                    "FIRST_VALUE",
                    "FLOAT",
                    "FOR",
                    "FORCE",
                    "FROM",
                    "FULL",
                    "FUNCTION",
                    // G
                    "GRANT",
                    "GROUP",
                    "GROUPS",
                    "GROUPING",
                    "GROUPING_ID",
                    // H
                    "HAVING",
                    "HLL",
                    "HOST",
                    // I
                    "IF",
                    "IGNORE",
                    "IN",
                    "INDEX",
                    "INFILE",
                    "INNER",
                    "INT",
                    "INTEGER",
                    "INTERSECT",
                    "INTO",
                    "IS",
                    "IMMEDIATE",
                    // J
                    "JOIN",
                    "JSON",
                    // K
                    "KEY",
                    "KEYS",
                    "KILL",
                    // L
                    "LAG",
                    "LARGEINT",
                    "LAST_VALUE",
                    "LATERAL",
                    "LEAD",
                    "LEFT",
                    "LIKE",
                    "LIMIT",
                    "LOAD",
                    "LOCALTIME",
                    "LOCALTIMESTAMP",
                    // M
                    "MAXVALUE",
                    "MINUS",
                    "MOD",
                    // N
                    "NTILE",
                    "NOT",
                    "NULL",
                    // O
                    "ON",
                    "OR",
                    "ORDER",
                    "OUTER",
                    "OUTFILE",
                    "OVER",
                    // P
                    "PARTITION",
                    "PERCENTILE",
                    "PRIMARY",
                    "PROCEDURE",
                    // Q
                    "QUALIFY",
                    // R
                    "RANGE",
                    "RANK",
                    "READ",
                    "REGEXP",
                    "RELEASE",
                    "RENAME",
                    "REPLACE",
                    "REVOKE",
                    "RIGHT",
                    "RLIEK",
                    "ROW",
                    "ROWS",
                    "ROW_NUMBER",
                    // S
                    "SCHEMA",
                    "SCHEMAS",
                    "SELECT",
                    "SET",
                    "SET_VAR",
                    "SHOW",
                    "SMALLINT",
                    "SYSTEM",
                    // T
                    "TABLE",
                    "TERMINATED",
                    "TEXT",
                    "THEN",
                    "TINYINT",
                    "TO",
                    "TRUE",
                    // U
                    "UNION",
                    "UNIQUE",
                    "UNSIGNED",
                    "UPDATE",
                    "USE",
                    "USING",
                    // V
                    "VALUES",
                    "VARCHAR",
                    // W
                    "WHEN",
                    "WHERE",
                    "WITH");
}
