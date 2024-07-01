package com.dtstack.flinkx.iceberg;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.iceberg.config.IcebergConfig;

import com.google.common.base.Preconditions;
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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        Configuration configuration = new Configuration();
        icebergConfig.getHadoopConfig().forEach((k, v) -> configuration.set(k, (String) v));

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
            default:
                throw new UnsupportedOperationException("Unsupported type -> `" + type + "`.");
        }
        return datatypeOpt.orElseThrow(
                () ->
                        new UnsupportedOperationException(
                                "Failed to convert type `" + type + "` into Flink datatype."));
    }

    public static List<Expression> parseSQLFilters(String whereClause) throws JSQLParserException {
        net.sf.jsqlparser.expression.Expression expr =
                CCJSqlParserUtil.parseCondExpression(whereClause);
        return Collections.singletonList(convertToIcebergExpression(expr));
    }

    private static Expression convertToIcebergExpression(
            net.sf.jsqlparser.expression.Expression expr) {
        if (expr instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expr;
            return Expressions.equal(
                    equalsTo.getLeftExpression().toString(),
                    getValue(equalsTo.getRightExpression()));
        } else if (expr instanceof GreaterThan) {
            GreaterThan greaterThan = (GreaterThan) expr;
            return Expressions.greaterThan(
                    greaterThan.getLeftExpression().toString(),
                    getValue(greaterThan.getRightExpression()));
        } else if (expr instanceof GreaterThanEquals) {
            GreaterThanEquals greaterThanEquals = (GreaterThanEquals) expr;
            return Expressions.greaterThanOrEqual(
                    greaterThanEquals.getLeftExpression().toString(),
                    getValue(greaterThanEquals.getRightExpression()));
        } else if (expr instanceof MinorThan) {
            MinorThan minorThan = (MinorThan) expr;
            return Expressions.lessThan(
                    minorThan.getLeftExpression().toString(),
                    getValue(minorThan.getRightExpression()));
        } else if (expr instanceof MinorThanEquals) {
            MinorThanEquals minorThanEquals = (MinorThanEquals) expr;
            return Expressions.lessThanOrEqual(
                    minorThanEquals.getLeftExpression().toString(),
                    getValue(minorThanEquals.getRightExpression()));
        } else if (expr instanceof NotEqualsTo) {
            NotEqualsTo notEqualsTo = (NotEqualsTo) expr;
            return Expressions.notEqual(
                    notEqualsTo.getLeftExpression().toString(),
                    getValue(notEqualsTo.getRightExpression()));
        } else if (expr instanceof InExpression) {
            InExpression inExpression = (InExpression) expr;
            String column = inExpression.getLeftExpression().toString();
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
                            between.getLeftExpression().toString(),
                            getValue(between.getBetweenExpressionStart())),
                    Expressions.lessThanOrEqual(
                            between.getLeftExpression().toString(),
                            getValue(between.getBetweenExpressionEnd())));
        } else if (expr instanceof IsNullExpression) {
            IsNullExpression isNullExpression = (IsNullExpression) expr;
            if (isNullExpression.isNot()) {
                return Expressions.notNull(isNullExpression.getLeftExpression().toString());
            } else {
                return Expressions.isNull(isNullExpression.getLeftExpression().toString());
            }
        } else if (expr instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expr;
            return Expressions.and(
                    convertToIcebergExpression(andExpression.getLeftExpression()),
                    convertToIcebergExpression(andExpression.getRightExpression()));
        } else if (expr instanceof OrExpression) {
            OrExpression orExpression = (OrExpression) expr;
            return Expressions.or(
                    convertToIcebergExpression(orExpression.getLeftExpression()),
                    convertToIcebergExpression(orExpression.getRightExpression()));
        } else if (expr instanceof Parenthesis) {
            Parenthesis parenthesis = (Parenthesis) expr;
            return convertToIcebergExpression(parenthesis.getExpression());
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
