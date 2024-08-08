package com.dtstack.flinkx.iceberg;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class IcebergUtilTest {

    @Test
    public void testParseWhereClauseAndConvertToIcebergFilters() throws JSQLParserException {
        final String filterSql =
                "age > 30 AND name = 'John' AND status IN ('ACTIVE', 'PENDING')\n"
                        + "OR (id > 10012 AND gender = 'male')";
        final Set<String> targetTableColumns =
                Sets.newSet("id", "name", "gender", "age", "description", "code", "status");

        List<Expression> filters = IcebergUtil.parseSQLFilters(filterSql, targetTableColumns);
        assertNotNull(filters);
        assertFalse(filters.isEmpty());
        assertEquals(1, filters.size());
        Expression expr = filters.get(0);
        assertEquals(Expression.Operation.OR, expr.op());
        Or or = (Or) expr;
        assertEquals(Expression.Operation.AND, or.left().op());
        assertEquals(Expression.Operation.AND, or.right().op());

        And leftAnd = (And) or.left();
        And rightAnd = (And) or.right();

        // verify right and expr
        assertEquals(Expression.Operation.GT, rightAnd.left().op());
        assertEquals("id", ((UnboundPredicate<?>) rightAnd.left()).ref().name());
        assertArrayEquals(
                new String[] {"10012"},
                ((UnboundPredicate<?>) rightAnd.left())
                        .literals().stream().map(Object::toString).toArray());

        assertEquals(Expression.Operation.EQ, rightAnd.right().op());
        assertEquals("gender", ((UnboundPredicate<?>) rightAnd.right()).ref().name());
        assertArrayEquals(
                new String[] {"\"male\""},
                ((UnboundPredicate<?>) rightAnd.right())
                        .literals().stream().map(Object::toString).toArray());

        // verify left and expr
        assertEquals(Expression.Operation.IN, leftAnd.right().op());
        assertEquals("status", ((UnboundPredicate<?>) leftAnd.right()).ref().name());
        assertArrayEquals(
                new String[] {"\"ACTIVE\"", "\"PENDING\""},
                ((UnboundPredicate<?>) leftAnd.right())
                        .literals().stream().map(Object::toString).toArray());

        assertEquals(Expression.Operation.AND, leftAnd.left().op());
        And leftAndLeftAnd = (And) leftAnd.left();
        assertEquals(Expression.Operation.EQ, leftAndLeftAnd.right().op());
        assertEquals("name", ((UnboundPredicate<?>) leftAndLeftAnd.right()).ref().name());
        assertArrayEquals(
                new String[] {"\"John\""},
                ((UnboundPredicate<?>) leftAndLeftAnd.right())
                        .literals().stream().map(Object::toString).toArray());

        assertEquals(Expression.Operation.GT, leftAndLeftAnd.left().op());
        assertEquals("age", ((UnboundPredicate<?>) leftAndLeftAnd.left()).ref().name());
        assertArrayEquals(
                new String[] {"30"},
                ((UnboundPredicate<?>) leftAndLeftAnd.left())
                        .literals().stream().map(Object::toString).toArray());
    }
}
