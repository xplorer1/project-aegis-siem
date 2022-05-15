package com.aegis.query;

import com.aegis.domain.QueryResult;
import com.aegis.domain.StorageTier;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

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
    private static final int CACHE_TTL_MINUTES = 5;
    private static final int CACHE_MAX_SIZE = 1000;
    private static final int DEFAULT_PAGE_SIZE = 1000;
    private static final int MAX_PAGE_SIZE = 10000;
    
    private final RestHighLevelClient openSearchClient;
    private final JdbcTemplate clickHouseTemplate;
    // Note: SparkSession will be added in future tasks for cold tier queries
    
    private final QueryMetrics metrics;
    private final ObjectMapper objectMapper;
    
    /**
     * Caffeine cache for query results
     * - Maximum size: 1000 entries
     * - TTL: 5 minutes after write
     * - Records cache statistics for monitoring
     */
    private final Cache<String, QueryResult> queryCache;
    
    /**
     * Constructor with dependency injection for storage tier clients
     * 
     * @param openSearchClient Client for hot tier (OpenSearch) queries
     * @param clickHouseTemplate JDBC template for warm tier (ClickHouse) queries
     * @param metrics Metrics collector for query execution
     */
    public QueryExecutor(
            RestHighLevelClient openSearchClient,
            @Qualifier("clickHouseJdbcTemplate") JdbcTemplate clickHouseTemplate,
            QueryMetrics metrics) {
        this.openSearchClient = openSearchClient;
        this.clickHouseTemplate = clickHouseTemplate;
        this.metrics = metrics;
        this.objectMapper = new ObjectMapper();
        
        // Initialize Caffeine cache with 5 minute TTL and max 1000 entries
        this.queryCache = Caffeine.newBuilder()
            .maximumSize(CACHE_MAX_SIZE)
            .expireAfterWrite(CACHE_TTL_MINUTES, TimeUnit.MINUTES)
            .recordStats()
            .build();
        
        log.info("QueryExecutor initialized with cache (TTL={}min, maxSize={})", 
            CACHE_TTL_MINUTES, CACHE_MAX_SIZE);
    }
    
    /**
     * Execute a query plan across all relevant storage tiers
     * 
     * This method:
     * 1. Generates a cache key from the query plan
     * 2. Checks the cache for existing results
     * 3. If cache miss, fans out sub-queries to all tiers concurrently
     * 4. Executes each sub-query using the appropriate tier-specific executor
     * 5. Merges results from all tiers into a single QueryResult
     * 6. Stores the result in cache for future queries (only if complete)
     * 
     * The execution is optimized for:
     * - Concurrent execution across tiers using parallel schedulers
     * - Timeout handling (30 seconds default) with partial result support
     * - Error isolation (one tier failure doesn't fail entire query)
     * - Efficient result merging with proper aggregation handling
     * - Query result caching with 5 minute TTL (only for complete results)
     * 
     * Timeout Behavior:
     * - Each sub-query has a 30-second timeout
     * - Overall query has a 35-second timeout (30s + 5s buffer)
     * - If any sub-query times out, partial results are returned
     * - The QueryResult will have timedOut=true and partialResults=true flags set
     * - Partial results are NOT cached to avoid serving incomplete data
     * - Metrics track timeout occurrences for monitoring and alerting
     * 
     * @param plan The query execution plan containing sub-queries for each tier
     * @return A Flux emitting the merged query result (possibly partial if timed out)
     */
    public Flux<QueryResult> execute(QueryPlan plan) {
        if (plan == null || plan.isEmpty()) {
            log.debug("Empty or null query plan, returning empty result");
            return Flux.just(new QueryResult());
        }
        
        // Generate cache key from query plan
        String cacheKey = generateCacheKey(plan);
        
        // Check cache first
        QueryResult cachedResult = queryCache.getIfPresent(cacheKey);
        if (cachedResult != null) {
            log.debug("Cache hit for query plan with {} sub-queries", plan.size());
            metrics.recordCacheHit();
            
            // Clone the cached result to avoid mutation
            QueryResult clonedResult = cloneCachedResult(cachedResult);
            clonedResult.setCached(true);
            return Flux.just(clonedResult);
        }
        
        log.info("Cache miss - executing query plan with {} sub-queries", plan.size());
        metrics.recordCacheMiss();
        long startTime = System.currentTimeMillis();
        
        // Track if we have partial results due to timeout
        final boolean[] timedOut = {false};
        
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
                .onErrorResume(java.util.concurrent.TimeoutException.class, error -> {
                    log.warn("Sub-query for tier {} timed out after {}s - returning partial results", 
                        subQuery.getTier(), DEFAULT_TIMEOUT.getSeconds());
                    timedOut[0] = true;
                    metrics.recordQueryTimedOut();
                    
                    // Return empty result for this tier but allow others to complete
                    QueryResult timeoutResult = new QueryResult();
                    timeoutResult.setStorageTier(subQuery.getTier());
                    timeoutResult.setTimedOut(true);
                    return Mono.just(timeoutResult);
                })
                .onErrorResume(error -> {
                    log.error("Error executing sub-query for tier {}: {}", 
                        subQuery.getTier(), error.getMessage(), error);
                    metrics.recordQueryError();
                    // Return empty result on error to allow other tiers to succeed
                    return Mono.just(new QueryResult());
                }))
            .sequential()
            .reduce(new QueryResult(), this::mergeResults)
            .doOnSuccess(result -> {
                long executionTime = System.currentTimeMillis() - startTime;
                result.setExecutionTimeMs(executionTime);
                result.setCached(false);
                
                // Mark if we had any timeouts
                if (timedOut[0]) {
                    result.setTimedOut(true);
                    result.setPartialResults(true);
                    log.warn("Query completed with timeout - returning partial results. " +
                        "Execution time: {}ms, Rows returned: {}", 
                        executionTime, result.getTotalCount());
                    // Don't cache partial results
                } else {
                    // Store in cache only if complete results
                    queryCache.put(cacheKey, result);
                    log.debug("Stored query result in cache with key: {}", cacheKey);
                    log.info("Query execution completed in {}ms with {} total rows", 
                        executionTime, result.getTotalCount());
                }
            })
            .timeout(DEFAULT_TIMEOUT.plusSeconds(5)) // Overall timeout with buffer
            .onErrorResume(java.util.concurrent.TimeoutException.class, error -> {
                // Overall query timeout - return whatever we have
                log.error("Overall query execution timed out after {}s", 
                    DEFAULT_TIMEOUT.plusSeconds(5).getSeconds());
                metrics.recordQueryTimedOut();
                
                QueryResult timeoutResult = new QueryResult();
                timeoutResult.setExecutionTimeMs(DEFAULT_TIMEOUT.plusSeconds(5).toMillis());
                timeoutResult.setTimedOut(true);
                timeoutResult.setPartialResults(true);
                return Mono.just(timeoutResult);
            })
            .flatMapMany(Flux::just);
    }
    
    /**
     * Generate a cache key from a query plan
     * 
     * The cache key is generated by combining:
     * - The number of sub-queries
     * - The tier for each sub-query
     * - The query string representation for each sub-query
     * 
     * This ensures that identical queries produce the same cache key,
     * while different queries produce different keys.
     * 
     * @param plan The query plan
     * @return A unique cache key string
     */
    private String generateCacheKey(QueryPlan plan) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append("plan:").append(plan.size()).append(":");
        
        // Sort sub-queries by tier to ensure consistent key generation
        plan.getSubQueries().stream()
            .sorted((a, b) -> a.getTier().compareTo(b.getTier()))
            .forEach(subQuery -> {
                keyBuilder.append(subQuery.getTier().name()).append(":");
                keyBuilder.append(subQuery.toString().hashCode()).append(":");
            });
        
        String cacheKey = keyBuilder.toString();
        log.trace("Generated cache key: {}", cacheKey);
        return cacheKey;
    }
    
    /**
     * Clone a cached query result to prevent mutation
     * 
     * This creates a shallow copy of the result with a new list of rows.
     * The individual row maps are not cloned, as they should be treated
     * as immutable.
     * 
     * @param cachedResult The cached result to clone
     * @return A cloned QueryResult
     */
    private QueryResult cloneCachedResult(QueryResult cachedResult) {
        QueryResult cloned = new QueryResult();
        cloned.setRows(new ArrayList<>(cachedResult.getRows()));
        cloned.setTotalCount(cachedResult.getTotalCount());
        cloned.setExecutionTimeMs(cachedResult.getExecutionTimeMs());
        cloned.setStorageTier(cachedResult.getStorageTier());
        return cloned;
    }
    
    /**
     * Get cache statistics for monitoring
     * 
     * Returns cache statistics including:
     * - Hit count
     * - Miss count
     * - Hit rate
     * - Eviction count
     * - Estimated size
     * 
     * @return A map of cache statistics
     */
    public Map<String, Object> getCacheStats() {
        com.github.benmanes.caffeine.cache.stats.CacheStats stats = queryCache.stats();
        
        Map<String, Object> statsMap = new ConcurrentHashMap<>();
        statsMap.put("hitCount", stats.hitCount());
        statsMap.put("missCount", stats.missCount());
        statsMap.put("hitRate", stats.hitRate());
        statsMap.put("evictionCount", stats.evictionCount());
        statsMap.put("estimatedSize", queryCache.estimatedSize());
        statsMap.put("loadSuccessCount", stats.loadSuccessCount());
        statsMap.put("loadFailureCount", stats.loadFailureCount());
        statsMap.put("totalLoadTime", stats.totalLoadTime());
        statsMap.put("averageLoadPenalty", stats.averageLoadPenalty());
        
        return statsMap;
    }
    
    /**
     * Invalidate the entire query cache
     * 
     * This method clears all cached query results. It should be called
     * when data is updated or when cache invalidation is required.
     */
    public void invalidateCache() {
        long sizeBefore = queryCache.estimatedSize();
        queryCache.invalidateAll();
        log.info("Invalidated query cache, removed {} entries", sizeBefore);
        metrics.recordCacheInvalidation();
    }
    
    /**
     * Invalidate a specific cache entry
     * 
     * @param cacheKey The cache key to invalidate
     */
    public void invalidateCacheEntry(String cacheKey) {
        queryCache.invalidate(cacheKey);
        log.debug("Invalidated cache entry: {}", cacheKey);
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
     * This method:
     * - Executes the SQL query using JdbcTemplate
     * - Converts the ResultSet to QueryResult
     * - Handles errors and timeouts
     * 
     * The execution follows the same pattern as executeOpenSearch():
     * - Async execution using Mono.fromCallable
     * - Comprehensive error handling with fallback to empty results
     * - 30 second timeout with partial result handling
     * - Detailed logging for debugging and monitoring
     * 
     * @param query The ClickHouse-specific query
     * @return A Mono emitting the query result from ClickHouse
     */
    private Mono<QueryResult> executeClickHouse(ClickHouseQuery query) {
        if (query == null || query.getSql() == null || query.getSql().isEmpty()) {
            log.warn("Received null or empty ClickHouse query, returning empty result");
            return Mono.just(new QueryResult());
        }
        
        long startTime = System.currentTimeMillis();
        String sql = query.getSql();
        
        log.debug("Executing ClickHouse query: {}", sql);
        
        return Mono.fromCallable(() -> {
            try {
                // Execute query using ResultSetExtractor to process the entire ResultSet
                QueryResult result = clickHouseTemplate.query(sql, rs -> {
                    QueryResult queryResult = new QueryResult();
                    queryResult.setStorageTier(StorageTier.WARM);
                    
                    List<Map<String, Object>> rows = new ArrayList<>();
                    
                    // Get column metadata
                    java.sql.ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    
                    // Process each row
                    while (rs.next()) {
                        Map<String, Object> row = new java.util.HashMap<>();
                        
                        // Extract all columns
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnLabel(i);
                            Object value = rs.getObject(i);
                            
                            // Handle special types
                            if (value instanceof java.sql.Timestamp) {
                                // Convert timestamp to ISO 8601 string
                                value = ((java.sql.Timestamp) value).toInstant().toString();
                            } else if (value instanceof java.sql.Date) {
                                // Convert date to ISO 8601 string
                                value = ((java.sql.Date) value).toLocalDate().toString();
                            } else if (value instanceof java.sql.Time) {
                                // Convert time to string
                                value = ((java.sql.Time) value).toLocalTime().toString();
                            } else if (value instanceof java.sql.Array) {
                                // Convert SQL array to Java array
                                value = ((java.sql.Array) value).getArray();
                            }
                            
                            row.put(columnName, value);
                        }
                        
                        rows.add(row);
                    }
                    
                    queryResult.setRows(rows);
                    queryResult.setTotalCount(rows.size());
                    
                    return queryResult;
                });
                
                // Handle null result (shouldn't happen, but be defensive)
                if (result == null) {
                    log.warn("ClickHouse query returned null result");
                    result = new QueryResult();
                    result.setStorageTier(StorageTier.WARM);
                }
                
                return result;
                
            } catch (Exception e) {
                log.error("Error executing ClickHouse query: {}", e.getMessage(), e);
                throw e;
            }
        })
        .subscribeOn(Schedulers.boundedElastic()) // Use bounded elastic for JDBC operations
        .map(result -> {
            // Set execution time
            long executionTime = System.currentTimeMillis() - startTime;
            result.setExecutionTimeMs(executionTime);
            
            log.debug("ClickHouse query completed in {}ms, returned {} rows", 
                executionTime, result.getTotalCount());
            
            return result;
        })
        .onErrorResume(e -> {
            // Log error and return empty result
            log.error("ClickHouse query execution failed: {}", e.getMessage(), e);
            QueryResult errorResult = new QueryResult();
            errorResult.setStorageTier(StorageTier.WARM);
            errorResult.setExecutionTimeMs(System.currentTimeMillis() - startTime);
            return Mono.just(errorResult);
        })
        .timeout(java.time.Duration.ofSeconds(30))
        .onErrorResume(java.util.concurrent.TimeoutException.class, e -> {
            // Handle timeout - return partial results if available
            log.warn("ClickHouse query timed out after 30 seconds: {}", sql);
            QueryResult timeoutResult = new QueryResult();
            timeoutResult.setStorageTier(StorageTier.WARM);
            timeoutResult.setExecutionTimeMs(30000);
            return Mono.just(timeoutResult);
        });
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
     *    - Maintains chronological order by timestamp
     *    - Updates total count
     *    - Handles pagination metadata
     * 
     * 2. For aggregation queries:
     *    - Merges aggregation buckets by key
     *    - Sums numeric aggregations (count, sum)
     *    - Recalculates derived aggregations (avg, min, max)
     *    - Handles nested aggregations recursively
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
            // Merge raw query results with proper ordering
            mergeRawResults(accumulator, result);
        }
        
        // Merge pagination metadata
        mergePaginationMetadata(accumulator, result);
        
        // Track if any tier had partial results or timeouts
        if (result.isPartialResults()) {
            accumulator.setPartialResults(true);
        }
        if (result.isTimedOut()) {
            accumulator.setTimedOut(true);
        }
        
        return accumulator;
    }
    
    /**
     * Merge raw query results with proper chronological ordering
     * 
     * This method:
     * - Concatenates rows from multiple tiers
     * - Sorts by timestamp field if present
     * - Updates total count
     * - Preserves tier information
     * 
     * @param accumulator The accumulated result
     * @param result The new result to merge
     */
    private void mergeRawResults(QueryResult accumulator, QueryResult result) {
        // Add all rows from the new result
        accumulator.addAll(result.getRows());
        accumulator.setTotalCount(accumulator.getTotalCount() + result.getTotalCount());
        
        // Sort by timestamp if present (descending - most recent first)
        // This ensures proper ordering when merging results from multiple tiers
        List<Map<String, Object>> rows = accumulator.getRows();
        if (!rows.isEmpty() && rows.get(0).containsKey("time")) {
            rows.sort((row1, row2) -> {
                Object time1 = row1.get("time");
                Object time2 = row2.get("time");
                
                if (time1 == null && time2 == null) return 0;
                if (time1 == null) return 1;
                if (time2 == null) return -1;
                
                // Compare timestamps (assuming ISO 8601 strings or comparable objects)
                if (time1 instanceof Comparable && time2 instanceof Comparable) {
                    @SuppressWarnings("unchecked")
                    Comparable<Object> c1 = (Comparable<Object>) time1;
                    @SuppressWarnings("unchecked")
                    Comparable<Object> c2 = (Comparable<Object>) time2;
                    return c2.compareTo(c1); // Descending order
                }
                
                // Fallback to string comparison
                return time2.toString().compareTo(time1.toString());
            });
        }
        
        log.debug("Merged raw results: {} total rows", accumulator.getTotalCount());
    }
    
    /**
     * Merge pagination metadata from multiple tier results
     * 
     * This method:
     * - Combines hasMore flags (true if any tier has more)
     * - Preserves cursor information
     * - Updates page information
     * 
     * @param accumulator The accumulated result
     * @param result The new result to merge
     */
    private void mergePaginationMetadata(QueryResult accumulator, QueryResult result) {
        // If any tier has more results, set hasMore to true
        if (result.isHasMore()) {
            accumulator.setHasMore(true);
        }
        
        // Preserve cursor if present (for cursor-based pagination)
        if (result.getCursor() != null && !result.getCursor().isEmpty()) {
            // Store cursors from all tiers (comma-separated)
            String existingCursor = accumulator.getCursor();
            if (existingCursor == null || existingCursor.isEmpty()) {
                accumulator.setCursor(result.getCursor());
            } else {
                accumulator.setCursor(existingCursor + "," + result.getCursor());
            }
        }
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
     * - Handles nested aggregations recursively
     * - Merges bucket-based aggregations (terms, histogram)
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
        
        // Check if results contain OpenSearch-style aggregations (with _aggregations key)
        boolean hasOpenSearchAggs = result.getRows().stream()
            .anyMatch(row -> row.containsKey("_aggregations"));
        
        if (hasOpenSearchAggs) {
            // Merge OpenSearch aggregation format
            mergeOpenSearchAggregations(accumulator, result);
        } else {
            // Merge standard aggregation format (ClickHouse/Spark)
            mergeStandardAggregations(accumulator, result);
        }
    }
    
    /**
     * Merge OpenSearch-style aggregations with nested bucket structure
     * 
     * OpenSearch aggregations have a special format with _aggregations key
     * containing nested bucket structures.
     * 
     * @param accumulator The accumulated result
     * @param result The new result to merge
     */
    private void mergeOpenSearchAggregations(QueryResult accumulator, QueryResult result) {
        // Find aggregation rows
        Map<String, Object> accAggRow = accumulator.getRows().stream()
            .filter(row -> row.containsKey("_aggregations"))
            .findFirst()
            .orElse(null);
        
        Map<String, Object> newAggRow = result.getRows().stream()
            .filter(row -> row.containsKey("_aggregations"))
            .findFirst()
            .orElse(null);
        
        if (accAggRow == null && newAggRow != null) {
            // First aggregation result
            accumulator.addRow(newAggRow);
        } else if (accAggRow != null && newAggRow != null) {
            // Merge aggregation structures
            @SuppressWarnings("unchecked")
            Map<String, Object> accAggs = (Map<String, Object>) accAggRow.get("_aggregations");
            @SuppressWarnings("unchecked")
            Map<String, Object> newAggs = (Map<String, Object>) newAggRow.get("_aggregations");
            
            // Merge each aggregation by name
            for (Map.Entry<String, Object> entry : newAggs.entrySet()) {
                String aggName = entry.getKey();
                Object newAggValue = entry.getValue();
                
                if (accAggs.containsKey(aggName)) {
                    // Merge existing aggregation
                    Object accAggValue = accAggs.get(aggName);
                    Object mergedAgg = mergeAggregationValue(accAggValue, newAggValue);
                    accAggs.put(aggName, mergedAgg);
                } else {
                    // New aggregation, add it
                    accAggs.put(aggName, newAggValue);
                }
            }
        }
    }
    
    /**
     * Merge two aggregation values (handles both metric and bucket aggregations)
     * 
     * @param accValue The accumulated aggregation value
     * @param newValue The new aggregation value to merge
     * @return The merged aggregation value
     */
    @SuppressWarnings("unchecked")
    private Object mergeAggregationValue(Object accValue, Object newValue) {
        // Handle numeric metric aggregations (count, sum, avg, etc.)
        if (accValue instanceof Number && newValue instanceof Number) {
            // For simple numeric values, sum them (works for count and sum)
            return ((Number) accValue).doubleValue() + ((Number) newValue).doubleValue();
        }
        
        // Handle bucket aggregations (terms, histogram, etc.)
        if (accValue instanceof Map && newValue instanceof Map) {
            Map<String, Object> accMap = (Map<String, Object>) accValue;
            Map<String, Object> newMap = (Map<String, Object>) newValue;
            
            // Check if this is a bucket aggregation (has "buckets" key)
            if (accMap.containsKey("buckets") && newMap.containsKey("buckets")) {
                mergeBucketAggregation(accMap, newMap);
                return accMap;
            }
            
            // Otherwise, merge map entries recursively
            for (Map.Entry<String, Object> entry : newMap.entrySet()) {
                String key = entry.getKey();
                Object newVal = entry.getValue();
                
                if (accMap.containsKey(key)) {
                    Object accVal = accMap.get(key);
                    accMap.put(key, mergeAggregationValue(accVal, newVal));
                } else {
                    accMap.put(key, newVal);
                }
            }
            return accMap;
        }
        
        // Default: return the new value
        return newValue;
    }
    
    /**
     * Merge bucket aggregations (terms, histogram, date_histogram, etc.)
     * 
     * Buckets are merged by their key, and doc_counts are summed.
     * Nested aggregations within buckets are also merged recursively.
     * 
     * @param accMap The accumulated bucket aggregation
     * @param newMap The new bucket aggregation to merge
     */
    @SuppressWarnings("unchecked")
    private void mergeBucketAggregation(Map<String, Object> accMap, Map<String, Object> newMap) {
        List<Map<String, Object>> accBuckets = (List<Map<String, Object>>) accMap.get("buckets");
        List<Map<String, Object>> newBuckets = (List<Map<String, Object>>) newMap.get("buckets");
        
        if (accBuckets == null || newBuckets == null) {
            return;
        }
        
        // Build a map of existing buckets by key
        Map<Object, Map<String, Object>> bucketMap = new HashMap<>();
        for (Map<String, Object> bucket : accBuckets) {
            Object key = bucket.get("key");
            if (key != null) {
                bucketMap.put(key, bucket);
            }
        }
        
        // Merge new buckets
        for (Map<String, Object> newBucket : newBuckets) {
            Object key = newBucket.get("key");
            if (key == null) continue;
            
            if (bucketMap.containsKey(key)) {
                // Merge with existing bucket
                Map<String, Object> accBucket = bucketMap.get(key);
                
                // Sum doc_count
                long accDocCount = getLongValue(accBucket, "doc_count");
                long newDocCount = getLongValue(newBucket, "doc_count");
                accBucket.put("doc_count", accDocCount + newDocCount);
                
                // Merge nested aggregations if present
                if (accBucket.containsKey("aggregations") && newBucket.containsKey("aggregations")) {
                    Map<String, Object> accSubAggs = (Map<String, Object>) accBucket.get("aggregations");
                    Map<String, Object> newSubAggs = (Map<String, Object>) newBucket.get("aggregations");
                    
                    for (Map.Entry<String, Object> entry : newSubAggs.entrySet()) {
                        String aggName = entry.getKey();
                        Object newAggValue = entry.getValue();
                        
                        if (accSubAggs.containsKey(aggName)) {
                            Object accAggValue = accSubAggs.get(aggName);
                            accSubAggs.put(aggName, mergeAggregationValue(accAggValue, newAggValue));
                        } else {
                            accSubAggs.put(aggName, newAggValue);
                        }
                    }
                }
            } else {
                // New bucket, add it
                bucketMap.put(key, newBucket);
            }
        }
        
        // Update buckets list with merged results
        accBuckets.clear();
        accBuckets.addAll(bucketMap.values());
        
        // Sort buckets by doc_count descending (most common first)
        accBuckets.sort((b1, b2) -> {
            long count1 = getLongValue(b1, "doc_count");
            long count2 = getLongValue(b2, "doc_count");
            return Long.compare(count2, count1); // Descending
        });
    }
    
    /**
     * Merge standard aggregation format (ClickHouse/Spark style)
     * 
     * Standard aggregations are rows with aggregation fields like count, sum, avg, etc.
     * and optional grouping fields.
     * 
     * @param accumulator The accumulated result
     * @param result The new result to merge
     */
    private void mergeStandardAggregations(QueryResult accumulator, QueryResult result) {
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
