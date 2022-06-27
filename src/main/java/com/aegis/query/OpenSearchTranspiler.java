package com.aegis.query;

import com.aegis.security.TenantContext;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Transpiles AQL queries to OpenSearch DSL for hot tier queries
 * 
 * This transpiler automatically injects tenant_id filters into all queries
 * to enforce multi-tenant isolation at the database driver level.
 */
@Component
public class OpenSearchTranspiler {

    private static final Logger logger = LoggerFactory.getLogger(OpenSearchTranspiler.class);

    /**
     * Transpile QueryContext to OpenSearch SearchSourceBuilder
     * 
     * Automatically injects tenant_id filter from TenantContext to ensure
     * multi-tenant isolation. All queries are scoped to the current tenant.
     */
    public SearchSourceBuilder transpileToOpenSearch(QueryContext context) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        
        // Build bool query
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        
        // CRITICAL: Inject tenant ID filter for multi-tenant isolation
        addTenantFilter(boolQuery);
        
        // Add time range filter
        addTimeRangeFilter(boolQuery, context.getTimeRange());
        
        // Add where clause filters
        if (context.getWhereExpression() != null) {
            transpileExpression(boolQuery, context.getWhereExpression());
        }
        
        searchSourceBuilder.query(boolQuery);
        
        // Add aggregations if stats clause present
        if (context.getStatsClause() != null) {
            addAggregations(searchSourceBuilder, context.getStatsClause());
            searchSourceBuilder.size(0); // Don't return documents, only aggregations
        }
        
        // Add sorting
        if (context.getSortClause() != null) {
            addSorting(searchSourceBuilder, context.getSortClause());
        }
        
        // Add limit
        if (context.getLimit() > 0) {
            searchSourceBuilder.size(context.getLimit());
        }
        
        return searchSourceBuilder;
    }

    /**
     * Add tenant ID filter to bool query for multi-tenant isolation
     * 
     * This method injects a tenant_id filter using the current tenant from TenantContext.
     * This ensures that all queries are automatically scoped to the current tenant,
     * preventing cross-tenant data access.
     * 
     * @param boolQuery The bool query to add the tenant filter to
     * @throws IllegalStateException if no tenant ID is set in the current context
     */
    private void addTenantFilter(BoolQueryBuilder boolQuery) {
        String tenantId = TenantContext.requireTenantId();
        logger.debug("Injecting tenant filter: tenant_id = {}", tenantId);
        boolQuery.filter(QueryBuilders.termQuery("tenant_id", tenantId));
    }

    /**
     * Add time range filter to bool query
     */
    private void addTimeRangeFilter(BoolQueryBuilder boolQuery, TimeRange timeRange) {
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("@timestamp")
                .gte(timeRange.getEarliest())
                .lte(timeRange.getLatest());
        boolQuery.filter(rangeQuery);
    }

    /**
     * Transpile expression to OpenSearch query
     */
    private void transpileExpression(BoolQueryBuilder boolQuery, Expression expression) {
        if (expression instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) expression;
            if ("AND".equals(binExpr.getOperator())) {
                transpileExpression(boolQuery, binExpr.getLeft());
                transpileExpression(boolQuery, binExpr.getRight());
            } else if ("OR".equals(binExpr.getOperator())) {
                BoolQueryBuilder orQuery = QueryBuilders.boolQuery();
                transpileExpression(orQuery, binExpr.getLeft());
                transpileExpression(orQuery, binExpr.getRight());
                boolQuery.should(orQuery);
            }
        } else if (expression instanceof ComparisonExpression) {
            ComparisonExpression compExpr = (ComparisonExpression) expression;
            addComparisonQuery(boolQuery, compExpr);
        }
    }

    /**
     * Add comparison query to bool query
     */
    private void addComparisonQuery(BoolQueryBuilder boolQuery, ComparisonExpression expr) {
        String field = expr.getField();
        String operator = expr.getOperator();
        String value = expr.getValue();
        
        switch (operator) {
            case "=" -> boolQuery.filter(QueryBuilders.termQuery(field + ".keyword", value));
            case "!=" -> boolQuery.mustNot(QueryBuilders.termQuery(field + ".keyword", value));
            case ">" -> boolQuery.filter(QueryBuilders.rangeQuery(field).gt(value));
            case "<" -> boolQuery.filter(QueryBuilders.rangeQuery(field).lt(value));
            case ">=" -> boolQuery.filter(QueryBuilders.rangeQuery(field).gte(value));
            case "<=" -> boolQuery.filter(QueryBuilders.rangeQuery(field).lte(value));
            case "contains" -> boolQuery.filter(QueryBuilders.matchQuery(field, value));
        }
    }

    /**
     * Add aggregations to search source builder
     */
    private void addAggregations(SearchSourceBuilder searchSourceBuilder, StatsClause statsClause) {
        // If there are group by fields, create a terms aggregation
        if (!statsClause.getGroupByFields().isEmpty()) {
            // Multi-level nested aggregations for group by
            AggregationBuilder rootAgg = null;
            AggregationBuilder currentAgg = null;
            
            for (String groupByField : statsClause.getGroupByFields()) {
                AggregationBuilder termsAgg = AggregationBuilders.terms(groupByField)
                        .field(groupByField + ".keyword")
                        .size(1000);
                
                if (rootAgg == null) {
                    rootAgg = termsAgg;
                    currentAgg = termsAgg;
                } else {
                    currentAgg.subAggregation(termsAgg);
                    currentAgg = termsAgg;
                }
            }
            
            // Add metric aggregations to the innermost group by
            for (Aggregation agg : statsClause.getAggregations()) {
                AggregationBuilder metricAgg = createMetricAggregation(agg);
                if (metricAgg != null) {
                    currentAgg.subAggregation(metricAgg);
                }
            }
            
            searchSourceBuilder.aggregation(rootAgg);
        } else {
            // No group by, just add metric aggregations
            for (Aggregation agg : statsClause.getAggregations()) {
                AggregationBuilder metricAgg = createMetricAggregation(agg);
                if (metricAgg != null) {
                    searchSourceBuilder.aggregation(metricAgg);
                }
            }
        }
    }

    /**
     * Create a metric aggregation from an Aggregation
     */
    private AggregationBuilder createMetricAggregation(Aggregation agg) {
        String function = agg.getFunction().toLowerCase();
        String field = agg.getField();
        String alias = agg.getAlias();
        
        return switch (function) {
            case "count" -> AggregationBuilders.count(alias).field(field);
            case "sum" -> AggregationBuilders.sum(alias).field(field);
            case "avg" -> AggregationBuilders.avg(alias).field(field);
            case "min" -> AggregationBuilders.min(alias).field(field);
            case "max" -> AggregationBuilders.max(alias).field(field);
            default -> null;
        };
    }

    /**
     * Add sorting to search source builder
     */
    private void addSorting(SearchSourceBuilder searchSourceBuilder, SortClause sortClause) {
        for (SortField sortField : sortClause.getSortFields()) {
            SortOrder order = sortField.isAscending() ? SortOrder.ASC : SortOrder.DESC;
            searchSourceBuilder.sort(sortField.getField(), order);
        }
    }
}
