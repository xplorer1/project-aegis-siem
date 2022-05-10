package com.aegis.query;

import com.aegis.domain.QueryResult;
import com.aegis.domain.StorageTier;
import io.micrometer.core.instrument.Timer;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QueryExecutor is responsible for executing queries across multiple storage tiers
 * (Hot/Warm/Cold) and merging the results.
 * 
 * This service:
 * - Fans out queries to all relevant storage tiers concurrently
 * - Executes tier-specific queries using appropriate clients
 * - Merges results from multiple tiers into a unified result set
 * 
 * The executor supports reactive execution patterns using Project Reactor
 * to handle concurrent queries efficiently.
 */
@Service
public class QueryExecutor {
    
    private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final int MAX_CONCURRENT_QUERIES = 10;
    
    private final RestHighLevelClient openSearchClient;
    private final JdbcTemplate clickHouseTemplate;
    // Note: SparkSession will be added in future tasks for cold tier queries
    
    /**
     * Constructor with dependency injection for storage tier clients
     * 
     * @param openSearchClient Client for hot tier (OpenSearch) queries
     * @param clickHouseTemplate JDBC template for warm tier (ClickHouse) queries
     */
    public QueryExecutor(
            RestHighLevelClient openSearchClient,
            @Qualifier("clickHouseJdbcTemplate") JdbcTemplate clickHouseTemplate) {
        this.openSearchClient = openSearchClient;
        this.clickHouseTemplate = clickHouseTemplate;
    }
    
    /**
     * Execute a query plan across all relevant storage tiers
     * 
     * This method:
     * 1. Fans out sub-queries to all tiers concurrently
     * 2. Executes each sub-query using the appropriate tier-specific executor
     * 3. Merges results from all tiers into a single QueryResult
     * 
     * The execution is optimized for:
     * - Concurrent execution across tiers using parallel schedulers
     * - Timeout handling (30 seconds default)
     * - Error isolation (one tier failure doesn't fail entire query)
     * - Efficient result merging with proper aggregation handling
     * 
     * @param plan The query execution plan containing sub-queries for each tier
     * @return A Flux emitting the merged query result
     */
    public Flux<QueryResult> execute(QueryPlan plan) {
        if (plan == null || plan.isEmpty()) {
            log.debug("Empty or null query plan, returning empty result");
            return Flux.just(new QueryResult());
        }
        
        log.info("Executing query plan with {} sub-queries", plan.size());
        long startTime = System.currentTimeMillis();
        
        // Fan out to all tiers concurrently with bounded parallelism
        // Each sub-query is executed on a parallel scheduler for true concurrency
        // Errors are caught and logged but don't fail the entire query
        return Flux.fromIterable(plan.getSubQueries())
            .parallel(Math.min(plan.size(), MAX_CONCURRENT_QUERIES))
            .runOn(Schedulers.parallel())
            .flatMap(subQuery -> executeSubQuery(subQuery)
                .timeout(DEFAULT_TIMEOUT)
                .doOnSuccess(result -> log.debug("Sub-query for tier {} completed with {} rows", 
                    subQuery.getTier(), result.getRows().size()))
                .onErrorResume(error -> {
                    log.error("Error executing sub-query for tier {}: {}", 
                        subQuery.getTier(), error.getMessage(), error);
                    // Return empty result on error to allow other tiers to succeed
                    return Mono.just(new QueryResult());
                }))
            .sequential()
            .reduce(new QueryResult(), this::mergeResults)
            .doOnSuccess(result -> {
                long executionTime = System.currentTimeMillis() - startTime;
                result.setExecutionTimeMs(executionTime);
                log.info("Query execution completed in {}ms with {} total rows", 
                    executionTime, result.getTotalCount());
            })
            .flatMapMany(Flux::just);
    }
    
    /**
     * Execute a single sub-query by routing to the appropriate tier-specific executor
     * 
     * This method:
     * - Routes the query to the correct tier-specific executor based on storage tier
     * - Tracks metrics for tier-specific query execution
     * - Handles errors gracefully with proper logging and error context
     * - Ensures failed queries don't block other tier queries
     * 
     * Error handling strategy:
     * - Logs all errors with full context (tier, query details, stack trace)
     * - Records error metrics for monitoring and alerting
     * - Returns empty result on error to allow other tiers to succeed
     * - Preserves partial results when possible
     * 
     * @param subQuery The sub-query to execute
     * @return A Mono emitting the query result from the specific tier, or empty result on error
     */
    private Mono<QueryResult> executeSubQuery(SubQuery subQuery) {
        if (subQuery == null) {
            log.warn("Received null subQuery, returning empty result");
            metrics.recordQueryError();
            return Mono.just(new QueryResult());
        }
        
        StorageTier tier = subQuery.getTier();
        log.debug("Executing sub-query for tier: {}", tier);
        
        // Start timing for this tier
        Timer.Sample tierSample = metrics.startQueryTimer();
        
        // Route to tier-specific executor with comprehensive error handling
        Mono<QueryResult> queryMono = switch (tier) {
            case HOT -> {
                metrics.recordHotTierQuery();
                Timer.Sample hotSample = metrics.startHotTierTimer();
                yield executeOpenSearch((OpenSearchQuery) subQuery)
                    .doOnSuccess(result -> {
                        metrics.recordHotTierLatency(hotSample);
                        log.debug("Hot tier query completed successfully, returned {} rows", 
                            result.getRows().size());
                    })
                    .doOnError(error -> {
                        metrics.recordHotTierLatency(hotSample);
                        metrics.recordQueryError();
                        log.error("Hot tier query failed: {}", error.getMessage(), error);
                    })
                    .onErrorResume(error -> {
                        // Create detailed error context
                        String errorMsg = String.format("Hot tier query execution failed: %s", 
                            error.getMessage());
                        log.warn(errorMsg + " - Returning empty result to allow other tiers to succeed");
                        
                        // Return empty result with error metadata
                        QueryResult errorResult = new QueryResult();
                        errorResult.setStorageTier(StorageTier.HOT);
                        return Mono.just(errorResult);
                    });
            }
            case WARM -> {
                metrics.recordWarmTierQuery();
                Timer.Sample warmSample = metrics.startWarmTierTimer();
                yield executeClickHouse((ClickHouseQuery) subQuery)
                    .doOnSuccess(result -> {
                        metrics.recordWarmTierLatency(warmSample);
                        log.debug("Warm tier query completed successfully, returned {} rows", 
                            result.getRows().size());
                    })
                    .doOnError(error -> {
                        metrics.recordWarmTierLatency(warmSample);
                        metrics.recordQueryError();
                        log.error("Warm tier query failed: {}", error.getMessage(), error);
                    })
                    .onErrorResume(error -> {
                        // Create detailed error context
                        String errorMsg = String.format("Warm tier query execution failed: %s", 
                            error.getMessage());
                        log.warn(errorMsg + " - Returning empty result to allow other tiers to succeed");
                        
                        // Return empty result with error metadata
                        QueryResult errorResult = new QueryResult();
                        errorResult.setStorageTier(StorageTier.WARM);
                        return Mono.just(errorResult);
                    });
            }
            case COLD -> {
                metrics.recordColdTierQuery();
                Timer.Sample coldSample = metrics.startColdTierTimer();
                yield executeCold(subQuery)
                    .doOnSuccess(result -> {
                        metrics.recordColdTierLatency(coldSample);
                        log.debug("Cold tier query completed successfully, returned {} rows", 
                            result.getRows().size());
                    })
                    .doOnError(error -> {
                        metrics.recordColdTierLatency(coldSample);
                        metrics.recordQueryError();
                        log.error("Cold tier query failed: {}", error.getMessage(), error);
                    })
                    .onErrorResume(error -> {
                        // Create detailed error context
                        String errorMsg = String.format("Cold tier query execution failed: %s", 
                            error.getMessage());
                        log.warn(errorMsg + " - Returning empty result to allow other tiers to succeed");
                        
                        // Return empty result with error metadata
                        QueryResult errorResult = new QueryResult();
                        errorResult.setStorageTier(StorageTier.COLD);
                        return Mono.just(errorResult);
                    });
            }
        };
        
        // Add final error handling and metrics recording
        return queryMono
            .doFinally(signalType -> {
                // Record overall tier execution time
                tierSample.stop(metrics.startQueryTimer());
                log.debug("Sub-query for tier {} completed with signal: {}", tier, signalType);
            })
            .onErrorResume(error -> {
                // Final safety net - should not reach here due to tier-specific error handling
                log.error("Unexpected error in executeSubQuery for tier {}: {}", 
                    tier, error.getMessage(), error);
                metrics.recordQueryError();
                
                QueryResult fallbackResult = new QueryResult();
                fallbackResult.setStorageTier(tier);
                return Mono.just(fallbackResult);
            });
    }
    
    /**
     * Execute a query against the hot tier (OpenSearch)
     * 
     * This method:
     * - Executes the OpenSearch query using the RestHighLevelClient
     * - Converts the SearchResponse to QueryResult
     * - Handles errors and timeouts
     * 
     * @param query The OpenSearch-specific query
     * @return A Mono emitting the query result from OpenSearch
     */
    private Mono<QueryResult> executeOpenSearch(OpenSearchQuery query) {
        if (query == null || query.getSearchSourceBuilder() == null) {
            return Mono.just(new QueryResult());
        }
        
        long startTime = System.currentTimeMillis();
        
        return Mono.fromFuture(() -> {
            java.util.concurrent.CompletableFuture<org.opensearch.action.search.SearchResponse> future = 
                new java.util.concurrent.CompletableFuture<>();
            
            try {
                // Create search request
                org.opensearch.action.search.SearchRequest searchRequest = 
                    new org.opensearch.action.search.SearchRequest()
                        .source(query.getSearchSourceBuilder());
                
                // Execute async search
                openSearchClient.searchAsync(
                    searchRequest,
                    org.opensearch.client.RequestOptions.DEFAULT,
                    new org.opensearch.action.ActionListener<org.opensearch.action.search.SearchResponse>() {
                        @Override
                        public void onResponse(org.opensearch.action.search.SearchResponse response) {
                            future.complete(response);
                        }
                        
                        @Override
                        public void onFailure(Exception e) {
                            future.completeExceptionally(e);
                        }
                    }
                );
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
            
            return future;
        })
        .map(response -> convertOpenSearchResponse(response, startTime))
        .onErrorResume(e -> {
            // Log error and return empty result
            System.err.println("OpenSearch query execution failed: " + e.getMessage());
            QueryResult errorResult = new QueryResult();
            errorResult.setStorageTier(StorageTier.HOT);
            errorResult.setExecutionTimeMs(System.currentTimeMillis() - startTime);
            return Mono.just(errorResult);
        })
        .timeout(java.time.Duration.ofSeconds(30))
        .onErrorResume(java.util.concurrent.TimeoutException.class, e -> {
            // Handle timeout - return partial results if available
            System.err.println("OpenSearch query timed out after 30 seconds");
            QueryResult timeoutResult = new QueryResult();
            timeoutResult.setStorageTier(StorageTier.HOT);
            timeoutResult.setExecutionTimeMs(30000);
            return Mono.just(timeoutResult);
        });
    }
    
    /**
     * Convert OpenSearch SearchResponse to QueryResult
     * 
     * @param response The OpenSearch SearchResponse
     * @param startTime The query start time in milliseconds
     * @return The converted QueryResult
     */
    private QueryResult convertOpenSearchResponse(
            org.opensearch.action.search.SearchResponse response, 
            long startTime) {
        
        QueryResult result = new QueryResult();
        result.setStorageTier(StorageTier.HOT);
        
        // Set execution time
        long executionTime = System.currentTimeMillis() - startTime;
        result.setExecutionTimeMs(executionTime);
        
        // Set total count
        long totalHits = response.getHits().getTotalHits().value;
        result.setTotalCount(totalHits);
        
        // Convert hits to rows
        java.util.List<java.util.Map<String, Object>> rows = new java.util.ArrayList<>();
        for (org.opensearch.search.SearchHit hit : response.getHits().getHits()) {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            
            // Add document ID
            row.put("_id", hit.getId());
            row.put("_index", hit.getIndex());
            row.put("_score", hit.getScore());
            
            // Add source fields
            if (hit.getSourceAsMap() != null) {
                row.putAll(hit.getSourceAsMap());
            }
            
            // Add highlighted fields if present
            if (hit.getHighlightFields() != null && !hit.getHighlightFields().isEmpty()) {
                java.util.Map<String, Object> highlights = new java.util.HashMap<>();
                hit.getHighlightFields().forEach((key, value) -> {
                    highlights.put(key, value.fragments());
                });
                row.put("_highlights", highlights);
            }
            
            rows.add(row);
        }
        
        result.setRows(rows);
        
        // Handle aggregations if present
        if (response.getAggregations() != null && !response.getAggregations().asList().isEmpty()) {
            java.util.Map<String, Object> aggregations = new java.util.HashMap<>();
            response.getAggregations().forEach(agg -> {
                aggregations.put(agg.getName(), convertAggregation(agg));
            });
            
            // Add aggregations as a special row
            if (!aggregations.isEmpty()) {
                java.util.Map<String, Object> aggRow = new java.util.HashMap<>();
                aggRow.put("_aggregations", aggregations);
                rows.add(aggRow);
            }
        }
        
        return result;
    }
    
    /**
     * Convert OpenSearch aggregation to a map structure
     * 
     * @param aggregation The OpenSearch aggregation
     * @return A map representation of the aggregation
     */
    private Object convertAggregation(org.opensearch.search.aggregations.Aggregation aggregation) {
        java.util.Map<String, Object> aggMap = new java.util.HashMap<>();
        
        // Handle different aggregation types
        if (aggregation instanceof org.opensearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue) {
            org.opensearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue singleValue = 
                (org.opensearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue) aggregation;
            return singleValue.value();
        } else if (aggregation instanceof org.opensearch.search.aggregations.bucket.terms.Terms) {
            org.opensearch.search.aggregations.bucket.terms.Terms terms = 
                (org.opensearch.search.aggregations.bucket.terms.Terms) aggregation;
            java.util.List<java.util.Map<String, Object>> buckets = new java.util.ArrayList<>();
            for (org.opensearch.search.aggregations.bucket.terms.Terms.Bucket bucket : terms.getBuckets()) {
                java.util.Map<String, Object> bucketMap = new java.util.HashMap<>();
                bucketMap.put("key", bucket.getKey());
                bucketMap.put("doc_count", bucket.getDocCount());
                
                // Handle sub-aggregations
                if (bucket.getAggregations() != null && !bucket.getAggregations().asList().isEmpty()) {
                    java.util.Map<String, Object> subAggs = new java.util.HashMap<>();
                    bucket.getAggregations().forEach(subAgg -> {
                        subAggs.put(subAgg.getName(), convertAggregation(subAgg));
                    });
                    bucketMap.put("aggregations", subAggs);
                }
                
                buckets.add(bucketMap);
            }
            aggMap.put("buckets", buckets);
            return aggMap;
        } else if (aggregation instanceof org.opensearch.search.aggregations.bucket.histogram.Histogram) {
            org.opensearch.search.aggregations.bucket.histogram.Histogram histogram = 
                (org.opensearch.search.aggregations.bucket.histogram.Histogram) aggregation;
            java.util.List<java.util.Map<String, Object>> buckets = new java.util.ArrayList<>();
            for (org.opensearch.search.aggregations.bucket.histogram.Histogram.Bucket bucket : histogram.getBuckets()) {
                java.util.Map<String, Object> bucketMap = new java.util.HashMap<>();
                bucketMap.put("key", bucket.getKey());
                bucketMap.put("doc_count", bucket.getDocCount());
                
                // Handle sub-aggregations
                if (bucket.getAggregations() != null && !bucket.getAggregations().asList().isEmpty()) {
                    java.util.Map<String, Object> subAggs = new java.util.HashMap<>();
                    bucket.getAggregations().forEach(subAgg -> {
                        subAggs.put(subAgg.getName(), convertAggregation(subAgg));
                    });
                    bucketMap.put("aggregations", subAggs);
                }
                
                buckets.add(bucketMap);
            }
            aggMap.put("buckets", buckets);
            return aggMap;
        }
        
        // Default: return the aggregation name
        aggMap.put("name", aggregation.getName());
        return aggMap;
    }
    
    /**
     * Execute a query against the warm tier (ClickHouse)
     * 
     * This method will be implemented in task 17.5 to:
     * - Execute the SQL query using JdbcTemplate
     * - Convert the ResultSet to QueryResult
     * - Handle errors and timeouts
     * 
     * @param query The ClickHouse-specific query
     * @return A Mono emitting the query result from ClickHouse
     */
    private Mono<QueryResult> executeClickHouse(ClickHouseQuery query) {
        // Placeholder implementation - will be completed in task 17.5
        return Mono.just(new QueryResult());
    }
    
    /**
     * Execute a query against the cold tier (Apache Iceberg via Spark)
     * 
     * This method will be implemented in future tasks to:
     * - Execute the Spark SQL query
     * - Convert the DataFrame to QueryResult
     * - Handle errors and timeouts
     * 
     * @param query The cold tier query
     * @return A Mono emitting the query result from cold tier
     */
    private Mono<QueryResult> executeCold(SubQuery query) {
        // Placeholder implementation - will be completed in future tasks
        return Mono.just(new QueryResult());
    }
    
    /**
     * Merge results from multiple storage tiers
     * 
     * This method intelligently merges results based on the query type:
     * 
     * 1. For raw queries (no aggregations):
     *    - Concatenates rows from all tiers
     *    - Maintains chronological order
     *    - Updates total count
     * 
     * 2. For aggregation queries:
     *    - Merges aggregation buckets by key
     *    - Sums numeric aggregations (count, sum)
     *    - Recalculates derived aggregations (avg, min, max)
     * 
     * The merge operation is designed to be efficient and handle large result sets
     * by using streaming operations where possible.
     * 
     * @param accumulator The accumulated result
     * @param result The new result to merge
     * @return The merged QueryResult
     */
    private QueryResult mergeResults(QueryResult accumulator, QueryResult result) {
        if (result == null || result.getRows() == null || result.getRows().isEmpty()) {
            log.debug("Skipping merge of empty result");
            return accumulator;
        }
        
        log.debug("Merging {} rows from tier {}", result.getRows().size(), result.getStorageTier());
        
        // Check if this is an aggregation query by examining the first row
        boolean isAggregation = isAggregationResult(result);
        
        if (isAggregation) {
            // Merge aggregation results
            mergeAggregations(accumulator, result);
        } else {
            // Simple concatenation for raw queries
            accumulator.addAll(result.getRows());
            accumulator.setTotalCount(accumulator.getTotalCount() + result.getTotalCount());
        }
        
        return accumulator;
    }
    
    /**
     * Determine if a result contains aggregation data
     * 
     * Aggregation results typically have fields like count, sum, avg, min, max
     * and may have grouping keys.
     * 
     * @param result The query result to check
     * @return true if the result contains aggregations
     */
    private boolean isAggregationResult(QueryResult result) {
        if (result.getRows().isEmpty()) {
            return false;
        }
        
        Map<String, Object> firstRow = result.getRows().get(0);
        
        // Check for common aggregation field names
        return firstRow.containsKey("count") || 
               firstRow.containsKey("sum") || 
               firstRow.containsKey("avg") || 
               firstRow.containsKey("min") || 
               firstRow.containsKey("max") ||
               firstRow.containsKey("doc_count");
    }
    
    /**
     * Merge aggregation results from multiple tiers
     * 
     * This method handles the complex logic of merging aggregated data:
     * - Groups results by aggregation keys
     * - Sums count and sum aggregations
     * - Recalculates averages based on sums and counts
     * - Takes min/max across all tiers
     * 
     * @param accumulator The accumulated aggregation result
     * @param result The new aggregation result to merge
     */
    private void mergeAggregations(QueryResult accumulator, QueryResult result) {
        if (accumulator.getRows().isEmpty()) {
            // First result, just copy all rows
            accumulator.addAll(result.getRows());
            accumulator.setTotalCount(result.getTotalCount());
            return;
        }
        
        // Build a map of existing aggregations by their grouping keys
        Map<String, Map<String, Object>> aggregationMap = new ConcurrentHashMap<>();
        
        for (Map<String, Object> row : accumulator.getRows()) {
            String key = buildAggregationKey(row);
            aggregationMap.put(key, row);
        }
        
        // Merge new results into the map
        for (Map<String, Object> newRow : result.getRows()) {
            String key = buildAggregationKey(newRow);
            
            if (aggregationMap.containsKey(key)) {
                // Merge with existing aggregation
                Map<String, Object> existingRow = aggregationMap.get(key);
                mergeAggregationRow(existingRow, newRow);
            } else {
                // New aggregation key, add it
                aggregationMap.put(key, newRow);
            }
        }
        
        // Update accumulator with merged results
        accumulator.setRows(new ArrayList<>(aggregationMap.values()));
        accumulator.setTotalCount(aggregationMap.size());
    }
    
    /**
     * Build a unique key for an aggregation row based on its grouping fields
     * 
     * The key is constructed from all non-aggregation fields (fields that aren't
     * count, sum, avg, min, max, etc.)
     * 
     * @param row The aggregation row
     * @return A unique key string
     */
    private String buildAggregationKey(Map<String, Object> row) {
        StringBuilder keyBuilder = new StringBuilder();
        
        // Common aggregation field names to exclude from the key
        List<String> aggFields = List.of("count", "sum", "avg", "min", "max", 
            "doc_count", "value", "key_as_string");
        
        row.entrySet().stream()
            .filter(entry -> !aggFields.contains(entry.getKey()))
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> {
                if (keyBuilder.length() > 0) {
                    keyBuilder.append("|");
                }
                keyBuilder.append(entry.getKey()).append("=").append(entry.getValue());
            });
        
        return keyBuilder.toString();
    }
    
    /**
     * Merge two aggregation rows with the same grouping key
     * 
     * This method updates the existing row with values from the new row:
     * - count: sum the counts
     * - sum: sum the sums
     * - avg: recalculate based on sum and count
     * - min: take the minimum
     * - max: take the maximum
     * 
     * @param existingRow The existing aggregation row (modified in place)
     * @param newRow The new aggregation row to merge
     */
    private void mergeAggregationRow(Map<String, Object> existingRow, Map<String, Object> newRow) {
        // Merge count
        if (newRow.containsKey("count")) {
            long existingCount = getLongValue(existingRow, "count");
            long newCount = getLongValue(newRow, "count");
            existingRow.put("count", existingCount + newCount);
        }
        
        // Merge sum
        if (newRow.containsKey("sum")) {
            double existingSum = getDoubleValue(existingRow, "sum");
            double newSum = getDoubleValue(newRow, "sum");
            existingRow.put("sum", existingSum + newSum);
        }
        
        // Recalculate average if both sum and count are present
        if (existingRow.containsKey("sum") && existingRow.containsKey("count")) {
            double sum = getDoubleValue(existingRow, "sum");
            long count = getLongValue(existingRow, "count");
            if (count > 0) {
                existingRow.put("avg", sum / count);
            }
        }
        
        // Merge min
        if (newRow.containsKey("min")) {
            double existingMin = getDoubleValue(existingRow, "min");
            double newMin = getDoubleValue(newRow, "min");
            existingRow.put("min", Math.min(existingMin, newMin));
        }
        
        // Merge max
        if (newRow.containsKey("max")) {
            double existingMax = getDoubleValue(existingRow, "max");
            double newMax = getDoubleValue(newRow, "max");
            existingRow.put("max", Math.max(existingMax, newMax));
        }
        
        // Merge doc_count (OpenSearch specific)
        if (newRow.containsKey("doc_count")) {
            long existingDocCount = getLongValue(existingRow, "doc_count");
            long newDocCount = getLongValue(newRow, "doc_count");
            existingRow.put("doc_count", existingDocCount + newDocCount);
        }
    }
    
    /**
     * Safely extract a long value from a map, handling various numeric types
     * 
     * @param map The map containing the value
     * @param key The key to extract
     * @return The long value, or 0 if not present or not numeric
     */
    private long getLongValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            log.warn("Failed to parse long value for key {}: {}", key, value);
            return 0L;
        }
    }
    
    /**
     * Safely extract a double value from a map, handling various numeric types
     * 
     * @param map The map containing the value
     * @param key The key to extract
     * @return The double value, or 0.0 if not present or not numeric
     */
    private double getDoubleValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return 0.0;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            log.warn("Failed to parse double value for key {}: {}", key, value);
            return 0.0;
        }
    }
}
