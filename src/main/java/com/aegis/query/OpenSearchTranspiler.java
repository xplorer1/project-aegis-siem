package com.aegis.query;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.springframework.stereotype.Component;

/**
 * Transpiles AQL queries to OpenSearch DSL for hot tier queries
 */
@Component
public class OpenSearchTranspiler {

    /**
     * Transpile QueryContext to OpenSearch SearchSourceBuilder
     */
    public SearchSourceBuilder transpileToOpenSearch(QueryContext context) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        
        // Build bool query
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        
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
        // Implementation in subsequent tasks
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
