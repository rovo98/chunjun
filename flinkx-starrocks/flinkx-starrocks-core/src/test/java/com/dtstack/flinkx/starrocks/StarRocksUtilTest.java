package com.dtstack.flinkx.starrocks;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class StarRocksUtilTest {

    @Test
    public void backtickReservedKeywords() {
        String[] reservedKeywords =
                new String[] {
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
                    "WITH"
                };

        String[] backticks = new String[reservedKeywords.length];
        String[] expected = new String[reservedKeywords.length];
        for (int i = 0; i < reservedKeywords.length; i++) {
            String kw = reservedKeywords[i];
            String backticked = StarRocksUtil.backtickPossibleReservedKeyword(kw);
            backticks[i] = backticked;
            expected[i] = String.format("`%s`", kw);
        }
        assertArrayEquals(expected, backticks);
    }

    @Test
    public void passCase() {
        String[] input =
                new String[] {
                    "load_threshold", "high_value", "with_p", "update_user", "true_val",
                };
        String[] backticks = new String[input.length];
        for (int i = 0; i < input.length; i++) {
            String kw = input[i];
            String backticked = StarRocksUtil.backtickPossibleReservedKeyword(kw);
            backticks[i] = backticked;
        }
        assertArrayEquals(input, backticks);
    }
}
