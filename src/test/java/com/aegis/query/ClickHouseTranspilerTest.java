package com.aegis.query;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for ClickHouseTranspiler
 */
class ClickHouseTranspilerTest {

    private ClickHouseTranspiler transpiler;

    @BeforeEach
    void setUp() {
        transpiler = new ClickHouseTranspiler();
    }

    @Test
    void testWhereClauseWithTimeRange() {
        // Given: A query context with only time range
        QueryContext context = new QueryContext();
        context.setTimeRange(new TimeRange(1650000000000L, 1650086400000L));

        // When: Transpiling to ClickHouse SQL
        String sql = transpiler.transpileToClickHouse(context);

        // Then: SQL should contain time range filter
        assertThat(sql).contains("WHERE time >= '2022-04-15 06:40:00'");
        assertThat(sql).contains("AND time <= '2022-04-16 06:40:00'");
    }

    @Test
    void testWhereClauseWithTimeRangeAndExpression() {
        // Given: A query context with time range and where expression
        QueryContext context = new QueryContext();
        context.setTimeRange(new TimeRange(1650000000000L, 1650086400000L));
        
        ComparisonExpression expr = new ComparisonExpression("severity", ">=", "3");
        context.setWhereExpression(expr);

        // When: Transpiling to ClickHouse SQL
        String sql = transpiler.transpileToClickHouse(context);

        // Then: SQL should contain both time range and expression filter
        assertThat(sql).contains("WHERE time >= '2022-04-15 06:40:00'");
        assertThat(sql).contains("AND time <= '2022-04-16 06:40:00'");
        assertThat(sql).contains("AND (severity >= 3)");
    }

    @Test
    void testWhereClauseWithBinaryExpression() {
        // Given: A query context with binary expression (AND)
        QueryContext context = new QueryContext();
        context.setTimeRange(new TimeRange(1650000000000L, 1650086400000L));
        
        ComparisonExpression left = new ComparisonExpression("severity", ">=", "3");
        ComparisonExpression right = new ComparisonExpression("actor_user", "=", "admin");
        BinaryExpression expr = new BinaryExpression("AND", left, right);
        context.setWhereExpression(expr);

        // When: Transpiling to ClickHouse SQL
        String sql = transpiler.transpileToClickHouse(context);

        // Then: SQL should contain binary expression
        assertThat(sql).contains("WHERE time >= '2022-04-15 06:40:00'");
        assertThat(sql).contains("AND ((severity >= 3) AND (actor_user = 'admin'))");
    }

    @Test
    void testWhereClauseWithContainsOperator() {
        // Given: A query context with contains operator
        QueryContext context = new QueryContext();
        context.setTimeRange(new TimeRange(1650000000000L, 1650086400000L));
        
        ComparisonExpression expr = new ComparisonExpression("message", "contains", "error");
        context.setWhereExpression(expr);

        // When: Transpiling to ClickHouse SQL
        String sql = transpiler.transpileToClickHouse(context);

        // Then: SQL should convert contains to LIKE
        assertThat(sql).contains("AND (message LIKE '%error%')");
    }

    @Test
    void testWhereClauseWithOrExpression() {
        // Given: A query context with binary expression (OR)
        QueryContext context = new QueryContext();
        context.setTimeRange(new TimeRange(1650000000000L, 1650086400000L));
        
        ComparisonExpression left = new ComparisonExpression("severity", "=", "5");
        ComparisonExpression right = new ComparisonExpression("severity", "=", "4");
        BinaryExpression expr = new BinaryExpression("OR", left, right);
        context.setWhereExpression(expr);

        // When: Transpiling to ClickHouse SQL
        String sql = transpiler.transpileToClickHouse(context);

        // Then: SQL should contain OR expression
        assertThat(sql).contains("AND ((severity = 5) OR (severity = 4))");
    }

    @Test
    void testWhereClauseWithComplexNestedExpression() {
        // Given: A query context with nested binary expressions
        QueryContext context = new QueryContext();
        context.setTimeRange(new TimeRange(1650000000000L, 1650086400000L));
        
        // (severity >= 3 AND actor_user = 'admin') OR (severity = 5)
        ComparisonExpression comp1 = new ComparisonExpression("severity", ">=", "3");
        ComparisonExpression comp2 = new ComparisonExpression("actor_user", "=", "admin");
        BinaryExpression left = new BinaryExpression("AND", comp1, comp2);
        
        ComparisonExpression comp3 = new ComparisonExpression("severity", "=", "5");
        BinaryExpression expr = new BinaryExpression("OR", left, comp3);
        context.setWhereExpression(expr);

        // When: Transpiling to ClickHouse SQL
        String sql = transpiler.transpileToClickHouse(context);

        // Then: SQL should contain nested expression
        assertThat(sql).contains("WHERE time >= '2022-04-15 06:40:00'");
        assertThat(sql).contains("AND (((severity >= 3) AND (actor_user = 'admin')) OR (severity = 5))");
    }

    @Test
    void testFullQueryWithWhereClause() {
        // Given: A complete query context
        QueryContext context = new QueryContext();
        context.setTimeRange(new TimeRange(1650000000000L, 1650086400000L));
        
        ComparisonExpression expr = new ComparisonExpression("severity", ">=", "3");
        context.setWhereExpression(expr);
        
        context.setLimit(100);

        // When: Transpiling to ClickHouse SQL
        String sql = transpiler.transpileToClickHouse(context);

        // Then: SQL should be a complete valid query
        assertThat(sql).startsWith("SELECT *");
        assertThat(sql).contains("FROM aegis_events_warm");
        assertThat(sql).contains("WHERE time >= '2022-04-15 06:40:00'");
        assertThat(sql).contains("AND time <= '2022-04-16 06:40:00'");
        assertThat(sql).contains("AND (severity >= 3)");
        assertThat(sql).contains("LIMIT 100");
    }
}
