package com.aegis.query;

import com.aegis.security.TenantContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Transpiles AQL queries to ClickHouse SQL for warm tier queries
 * 
 * This transpiler automatically injects tenant_id filters into all queries
 * to enforce multi-tenant isolation at the database driver level.
 */
@Component
public class ClickHouseTranspiler {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseTranspiler.class);
    
    private static final DateTimeFormatter DATETIME_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    /**
     * Transpile QueryContext to ClickHouse SQL
     * 
     * Automatically injects tenant_id filter from TenantContext to ensure
     * multi-tenant isolation. All queries are scoped to the current tenant.
     */
    public String transpileToClickHouse(QueryContext context) {
        StringBuilder sql = new StringBuilder();
        
        // SELECT clause
        sql.append("SELECT ");
        if (context.getStatsClause() != null) {
            sql.append(buildSelectWithAggregations(context.getStatsClause()));
        } else {
            sql.append("*");
        }
        
        // FROM clause
        sql.append(" FROM aegis_events_warm");
        
        // WHERE clause
        sql.append(" WHERE ");
        sql.append(buildWhereClause(context));
        
        // GROUP BY clause
        if (context.getStatsClause() != null && !context.getStatsClause().getGroupByFields().isEmpty()) {
            sql.append(" GROUP BY ");
            sql.append(String.join(", ", context.getStatsClause().getGroupByFields()));
        }
        
        // ORDER BY clause
        if (context.getSortClause() != null) {
            sql.append(" ORDER BY ");
            sql.append(buildOrderByClause(context.getSortClause()));
        }
        
        // LIMIT clause
        if (context.getLimit() > 0) {
            sql.append(" LIMIT ").append(context.getLimit());
        }
        
        return sql.toString();
    }

    /**
     * Build SELECT clause with aggregations
     */
    private String buildSelectWithAggregations(StatsClause statsClause) {
        StringBuilder select = new StringBuilder();
        
        // Add group by fields first
        if (!statsClause.getGroupByFields().isEmpty()) {
            select.append(String.join(", ", statsClause.getGroupByFields()));
            select.append(", ");
        }
        
        // Add aggregation functions
        for (int i = 0; i < statsClause.getAggregations().size(); i++) {
            if (i > 0) select.append(", ");
            Aggregation agg = statsClause.getAggregations().get(i);
            select.append(buildAggregationFunction(agg));
        }
        
        return select.toString();
    }

    /**
     * Build aggregation function SQL
     */
    private String buildAggregationFunction(Aggregation agg) {
        String function = agg.getFunction().toUpperCase();
        String field = agg.getField();
        String alias = agg.getAlias();
        
        return function + "(" + field + ") AS " + alias;
    }

    /**
     * Build WHERE clause with tenant filter and other conditions
     * 
     * This method automatically injects the tenant_id filter to ensure
     * multi-tenant isolation. The tenant filter is always applied first.
     */
    private String buildWhereClause(QueryContext context) {
        StringBuilder where = new StringBuilder();
        
        // CRITICAL: Inject tenant ID filter for multi-tenant isolation
        String tenantId = TenantContext.requireTenantId();
        logger.debug("Injecting tenant filter: tenant_id = '{}'", tenantId);
        where.append("tenant_id = '").append(tenantId).append("'");
        
        // Time range filter
        TimeRange timeRange = context.getTimeRange();
        String earliest = DATETIME_FORMATTER.format(Instant.ofEpochMilli(timeRange.getEarliest()));
        String latest = DATETIME_FORMATTER.format(Instant.ofEpochMilli(timeRange.getLatest()));
        
        where.append(" AND time >= '").append(earliest).append("'");
        where.append(" AND time <= '").append(latest).append("'");
        
        // Additional where expressions
        if (context.getWhereExpression() != null) {
            where.append(" AND (");
            where.append(buildExpressionSql(context.getWhereExpression()));
            where.append(")");
        }
        
        return where.toString();
    }

    /**
     * Build SQL expression from Expression AST
     */
    private String buildExpressionSql(Expression expression) {
        if (expression instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) expression;
            String left = buildExpressionSql(binExpr.getLeft());
            String right = buildExpressionSql(binExpr.getRight());
            return "(" + left + " " + binExpr.getOperator() + " " + right + ")";
        } else if (expression instanceof ComparisonExpression) {
            ComparisonExpression compExpr = (ComparisonExpression) expression;
            return buildComparisonSql(compExpr);
        }
        return "";
    }

    /**
     * Build SQL comparison expression
     */
    private String buildComparisonSql(ComparisonExpression expr) {
        String field = expr.getField();
        String operator = expr.getOperator();
        String value = expr.getValue();
        
        // Handle contains operator
        if ("contains".equals(operator)) {
            return field + " LIKE '%" + value + "%'";
        }
        
        // Quote string values
        if (!value.matches("-?\\d+(\\.\\d+)?")) {
            value = "'" + value + "'";
        }
        
        return field + " " + operator + " " + value;
    }

    /**
     * Build ORDER BY clause
     */
    private String buildOrderByClause(SortClause sortClause) {
        StringBuilder orderBy = new StringBuilder();
        
        for (int i = 0; i < sortClause.getSortFields().size(); i++) {
            if (i > 0) orderBy.append(", ");
            SortField sortField = sortClause.getSortFields().get(i);
            orderBy.append(sortField.getField());
            orderBy.append(" ");
            orderBy.append(sortField.isAscending() ? "ASC" : "DESC");
        }
        
        return orderBy.toString();
    }
}
