package com.dtstack.flinkx.iceberg;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.iceberg.config.IcebergConfig;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class IcebergUtil {
    private IcebergUtil() {}

    public static TableLoader buildTableLoader(IcebergConfig icebergConfig) {
        Preconditions.checkNotNull(icebergConfig);
        Map<String, String> props =
                new HashMap<String, String>() {
                    {
                        put("warehouse", icebergConfig.getWarehouse());
                        put("uri", icebergConfig.getMetastoreUris());
                    }
                };
        // setup hadoop configuration
        Configuration configuration =
                loadAndMergeHiveConf(
                        icebergConfig.getHiveConfDir(), icebergConfig.getHadoopConfDir());
        if (Objects.nonNull(icebergConfig.getHadoopConfig())) {
            icebergConfig.getHadoopConfig().forEach((k, v) -> configuration.set(k, (String) v));
        }
        CatalogLoader hcl = CatalogLoader.hive(icebergConfig.getDatabase(), configuration, props);
        TableLoader tl =
                TableLoader.fromCatalog(
                        hcl,
                        TableIdentifier.of(icebergConfig.getDatabase(), icebergConfig.getTable()));
        if (tl instanceof TableLoader.CatalogTableLoader) {
            tl.open();
        }
        return tl;
    }

    private static Configuration loadAndMergeHiveConf(String hiveConfDir, String hadoopConfDir) {
        Configuration hdpConf = new Configuration();
        if (!Strings.isNullOrEmpty(hiveConfDir)) {
            Preconditions.checkState(
                    Files.exists(Paths.get(hiveConfDir, "hive-site.xml")),
                    "There should be a hive-site.xml file under the directory %s",
                    hiveConfDir);
            hdpConf.addResource(new Path(hiveConfDir, "hive-site.xml"));
        }
        if (!Strings.isNullOrEmpty(hadoopConfDir)) {
            java.nio.file.Path coreSiteConfFile = Paths.get(hadoopConfDir, "core-site.xml");
            java.nio.file.Path hdfsSiteConfFile = Paths.get(hadoopConfDir, "hdfs-site.xml");
            Preconditions.checkState(
                    Files.exists(coreSiteConfFile),
                    "Failed to load Hadoop configuration: missing %s",
                    coreSiteConfFile);
            Preconditions.checkState(
                    Files.exists(hdfsSiteConfFile),
                    "Failed to load Hadoop configuration: missing %s",
                    hdfsSiteConfFile);
            hdpConf.addResource(new Path(hadoopConfDir, "core-site.xml"));
            hdpConf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));
        }
        return hdpConf;
    }

    public static DataType internalType2FlinkDataType(String type) {
        ColumnType columnType = ColumnType.getType(type);
        Optional<DataType> datatypeOpt;
        switch (columnType) {
            case TINYINT:
                datatypeOpt = TypeConversions.fromClassToDataType(Byte.class);
                break;
            case SMALLINT:
                datatypeOpt = TypeConversions.fromClassToDataType(Short.class);
                break;
            case INT:
                datatypeOpt = TypeConversions.fromClassToDataType(Integer.class);
                break;
            case MEDIUMINT:
            case BIGINT:
                datatypeOpt = TypeConversions.fromClassToDataType(Long.class);
                break;
            case FLOAT:
                datatypeOpt = TypeConversions.fromClassToDataType(Float.class);
                break;
            case DOUBLE:
                datatypeOpt = TypeConversions.fromClassToDataType(Double.class);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                datatypeOpt = TypeConversions.fromClassToDataType(String.class);
                break;
            case BOOLEAN:
                datatypeOpt = TypeConversions.fromClassToDataType(Boolean.class);
                break;
            case DATE:
                datatypeOpt = TypeConversions.fromClassToDataType(Date.class);
                break;
            case TIME:
                datatypeOpt = TypeConversions.fromClassToDataType(Time.class);
                break;
            case TIMESTAMP:
            case DATETIME:
                datatypeOpt = TypeConversions.fromClassToDataType(Timestamp.class);
                break;
            case DECIMAL:
                datatypeOpt = TypeConversions.fromClassToDataType(BigDecimal.class);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type -> `" + type + "`.");
        }
        return datatypeOpt.orElseThrow(
                () ->
                        new UnsupportedOperationException(
                                "Failed to convert type `" + type + "` into Flink datatype."));
    }

    public static List<Expression> parseSQLFilters(String whereClause) throws JSQLParserException {
        return parseSQLFilters(whereClause, Collections.emptySet());
    }

    public static List<Expression> parseSQLFilters(
            String whereClause, Set<String> targetTableColumns) throws JSQLParserException {
        net.sf.jsqlparser.expression.Expression expr =
                CCJSqlParserUtil.parseCondExpression(whereClause);
        return Collections.singletonList(convertToIcebergExpression(expr, targetTableColumns));
    }

    private static String validateAndGetColumn(String col, Set<String> refTargetTblCols) {
        if (refTargetTblCols.isEmpty()) return col;
        if (!refTargetTblCols.contains(col)) {
            throw new ValidationException(
                    col
                            + " field NOT found in target table. Available fields are: "
                            + refTargetTblCols);
        }
        return col;
    }

    private static Expression convertToIcebergExpression(
            net.sf.jsqlparser.expression.Expression expr, Set<String> targetTableColumns) {
        if (expr instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expr;
            return Expressions.equal(
                    validateAndGetColumn(
                            equalsTo.getLeftExpression().toString(), targetTableColumns),
                    getValue(equalsTo.getRightExpression()));
        } else if (expr instanceof GreaterThan) {
            GreaterThan greaterThan = (GreaterThan) expr;
            return Expressions.greaterThan(
                    validateAndGetColumn(
                            greaterThan.getLeftExpression().toString(), targetTableColumns),
                    getValue(greaterThan.getRightExpression()));
        } else if (expr instanceof GreaterThanEquals) {
            GreaterThanEquals greaterThanEquals = (GreaterThanEquals) expr;
            return Expressions.greaterThanOrEqual(
                    validateAndGetColumn(
                            greaterThanEquals.getLeftExpression().toString(), targetTableColumns),
                    getValue(greaterThanEquals.getRightExpression()));
        } else if (expr instanceof MinorThan) {
            MinorThan minorThan = (MinorThan) expr;
            return Expressions.lessThan(
                    validateAndGetColumn(
                            minorThan.getLeftExpression().toString(), targetTableColumns),
                    getValue(minorThan.getRightExpression()));
        } else if (expr instanceof MinorThanEquals) {
            MinorThanEquals minorThanEquals = (MinorThanEquals) expr;
            return Expressions.lessThanOrEqual(
                    validateAndGetColumn(
                            minorThanEquals.getLeftExpression().toString(), targetTableColumns),
                    getValue(minorThanEquals.getRightExpression()));
        } else if (expr instanceof NotEqualsTo) {
            NotEqualsTo notEqualsTo = (NotEqualsTo) expr;
            return Expressions.notEqual(
                    validateAndGetColumn(
                            notEqualsTo.getLeftExpression().toString(), targetTableColumns),
                    getValue(notEqualsTo.getRightExpression()));
        } else if (expr instanceof InExpression) {
            InExpression inExpression = (InExpression) expr;
            String column =
                    validateAndGetColumn(
                            inExpression.getLeftExpression().toString(), targetTableColumns);
            List<Object> inExprValues = getInExprValues(inExpression.getRightExpression());
            if (inExpression.isNot()) {
                return Expressions.notIn(column, inExprValues);
            } else {
                return Expressions.in(column, inExprValues);
            }
        } else if (expr instanceof Between) {
            Between between = (Between) expr;
            return Expressions.and(
                    Expressions.greaterThanOrEqual(
                            validateAndGetColumn(
                                    between.getLeftExpression().toString(), targetTableColumns),
                            getValue(between.getBetweenExpressionStart())),
                    Expressions.lessThanOrEqual(
                            validateAndGetColumn(
                                    between.getLeftExpression().toString(), targetTableColumns),
                            getValue(between.getBetweenExpressionEnd())));
        } else if (expr instanceof IsNullExpression) {
            IsNullExpression isNullExpression = (IsNullExpression) expr;
            String column =
                    validateAndGetColumn(
                            isNullExpression.getLeftExpression().toString(), targetTableColumns);
            if (isNullExpression.isNot()) {
                return Expressions.notNull(column);
            } else {
                return Expressions.isNull(column);
            }
        } else if (expr instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expr;
            return Expressions.and(
                    convertToIcebergExpression(
                            andExpression.getLeftExpression(), targetTableColumns),
                    convertToIcebergExpression(
                            andExpression.getRightExpression(), targetTableColumns));
        } else if (expr instanceof OrExpression) {
            OrExpression orExpression = (OrExpression) expr;
            return Expressions.or(
                    convertToIcebergExpression(
                            orExpression.getLeftExpression(), targetTableColumns),
                    convertToIcebergExpression(
                            orExpression.getRightExpression(), targetTableColumns));
        } else if (expr instanceof Parenthesis) {
            Parenthesis parenthesis = (Parenthesis) expr;
            return convertToIcebergExpression(parenthesis.getExpression(), targetTableColumns);
        }
        throw new UnsupportedOperationException("Unsupported expression type: " + expr.getClass());
    }

    private static Object getValue(net.sf.jsqlparser.expression.Expression expr) {
        if (expr instanceof StringValue) {
            return ((StringValue) expr).getValue();
        } else if (expr instanceof LongValue) {
            return ((LongValue) expr).getValue();
        }
        throw new UnsupportedOperationException("Unsupported value type: " + expr.getClass());
    }

    private static List<Object> getInExprValues(net.sf.jsqlparser.expression.Expression expr) {
        if (expr instanceof ExpressionList) {
            return ((ExpressionList<?>) expr)
                    .stream().map(IcebergUtil::getValue).collect(Collectors.toList());
        }
        throw new UnsupportedOperationException("Unsupported items list type: " + expr.getClass());
    }
}
