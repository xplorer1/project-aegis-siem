package com.aegis.query;

import com.aegis.domain.QueryResult;
import com.aegis.domain.StorageTier;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
     * @param plan The query execution plan containing sub-queries for each tier
     * @return A Flux emitting the merged query result
     */
    public Flux<QueryResult> execute(QueryPlan plan) {
        if (plan == null || plan.isEmpty()) {
            return Flux.just(new QueryResult());
        }
        
        // Fan out to all tiers concurrently
        // Each sub-query is executed asynchronously and returns a Mono<QueryResult>
        return Flux.fromIterable(plan.getSubQueries())
            .flatMap(this::executeSubQuery)
            .reduce(new QueryResult(), this::mergeResults)
            .flatMapMany(Flux::just);
    }
    
    /**
     * Execute a single sub-query by routing to the appropriate tier-specific executor
     * 
     * @param subQuery The sub-query to execute
     * @return A Mono emitting the query result from the specific tier
     */
    private Mono<QueryResult> executeSubQuery(SubQuery subQuery) {
        if (subQuery == null) {
            return Mono.just(new QueryResult());
        }
        
        return switch (subQuery.getTier()) {
            case HOT -> executeOpenSearch((OpenSearchQuery) subQuery);
            case WARM -> executeClickHouse((ClickHouseQuery) subQuery);
            case COLD -> executeCold(subQuery);
        };
    }
    
    /**
     * Execute a query against the hot tier (OpenSearch)
     * 
     * This method will be implemented in task 17.4 to:
     * - Execute the OpenSearch query using the RestHighLevelClient
     * - Convert the SearchResponse to QueryResult
     * - Handle errors and timeouts
     * 
     * @param query The OpenSearch-specific query
     * @return A Mono emitting the query result from OpenSearch
     */
    private Mono<QueryResult> executeOpenSearch(OpenSearchQuery query) {
        // Placeholder implementation - will be completed in task 17.4
        return Mono.just(new QueryResult());
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
     * This method will be enhanced in task 17.6 to:
     * - Handle aggregation merging (sum counts, merge buckets)
     * - Handle raw query result concatenation
     * - Maintain proper ordering and pagination
     * 
     * @param accumulator The accumulated result
     * @param result The new result to merge
     * @return The merged QueryResult
     */
    private QueryResult mergeResults(QueryResult accumulator, QueryResult result) {
        // Simple concatenation for now - will be enhanced in task 17.6
        if (result != null && result.getRows() != null) {
            accumulator.addAll(result.getRows());
            accumulator.setTotalCount(accumulator.getTotalCount() + result.getTotalCount());
        }
        return accumulator;
    }
}
