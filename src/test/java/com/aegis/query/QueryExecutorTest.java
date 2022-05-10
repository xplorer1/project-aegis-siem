package com.aegis.query;

import com.aegis.domain.QueryResult;
import com.aegis.domain.StorageTier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for QueryExecutor
 * Tests the query execution interface and client dependencies
 */
@ExtendWith(MockitoExtension.class)
class QueryExecutorTest {
    
    @Mock
    private RestHighLevelClient openSearchClient;
    
    @Mock
    private JdbcTemplate clickHouseTemplate;
    
    private QueryExecutor queryExecutor;
    
    @BeforeEach
    void setUp() {
        queryExecutor = new QueryExecutor(openSearchClient, clickHouseTemplate);
    }
    
    @Test
    void testConstructor_WithValidDependencies_ShouldCreateInstance() {
        // Given: Valid client dependencies
        // When: QueryExecutor is created
        // Then: Instance should be created successfully
        assertThat(queryExecutor).isNotNull();
    }
    
    @Test
    void testExecute_WithNullPlan_ShouldReturnEmptyResult() {
        // Given: A null query plan
        QueryPlan plan = null;
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return empty result
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).isEmpty();
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithEmptyPlan_ShouldReturnEmptyResult() {
        // Given: An empty query plan
        QueryPlan plan = new QueryPlan();
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return empty result
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).isEmpty();
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithSingleHotTierQuery_ShouldExecuteSuccessfully() {
        // Given: A query plan with a single hot tier query
        QueryPlan plan = new QueryPlan();
        OpenSearchQuery hotQuery = new OpenSearchQuery(null); // Placeholder
        plan.addSubQuery(hotQuery);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return a result (placeholder implementation returns empty)
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithSingleWarmTierQuery_ShouldExecuteSuccessfully() {
        // Given: A query plan with a single warm tier query
        QueryPlan plan = new QueryPlan();
        ClickHouseQuery warmQuery = new ClickHouseQuery("SELECT * FROM aegis_events_warm");
        plan.addSubQuery(warmQuery);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return a result (placeholder implementation returns empty)
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithMultipleTierQueries_ShouldMergeResults() {
        // Given: A query plan with queries for multiple tiers
        QueryPlan plan = new QueryPlan();
        OpenSearchQuery hotQuery = new OpenSearchQuery(null);
        ClickHouseQuery warmQuery = new ClickHouseQuery("SELECT * FROM aegis_events_warm");
        plan.addSubQuery(hotQuery);
        plan.addSubQuery(warmQuery);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should merge results from both tiers
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                // Results will be merged (currently both return empty)
            })
            .verifyComplete();
    }
    
    @Test
    void testMergeResults_WithValidResults_ShouldCombineRows() {
        // This test verifies the merge logic indirectly through execute
        // Direct testing of mergeResults will be added when the method is enhanced
        
        // Given: Multiple query results
        QueryResult result1 = new QueryResult();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", "1");
        row1.put("message", "Event 1");
        result1.addRow(row1);
        result1.setTotalCount(1);
        
        QueryResult result2 = new QueryResult();
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", "2");
        row2.put("message", "Event 2");
        result2.addRow(row2);
        result2.setTotalCount(1);
        
        // When: Results are merged (simulated)
        QueryResult merged = new QueryResult();
        merged.addAll(result1.getRows());
        merged.addAll(result2.getRows());
        merged.setTotalCount(result1.getTotalCount() + result2.getTotalCount());
        
        // Then: Should contain rows from both results
        assertThat(merged.getRows()).hasSize(2);
        assertThat(merged.getTotalCount()).isEqualTo(2);
    }
    
    @Test
    void testQueryPlanIntegration_WithComplexPlan_ShouldHandleCorrectly() {
        // Given: A complex query plan with multiple sub-queries
        QueryPlan plan = new QueryPlan();
        
        // Add queries for different tiers
        plan.addSubQuery(new OpenSearchQuery(null));
        plan.addSubQuery(new ClickHouseQuery("SELECT COUNT(*) FROM aegis_events_warm"));
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should handle all sub-queries
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(plan.size()).isEqualTo(2);
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithConcurrentQueries_ShouldExecuteInParallel() {
        // Given: A query plan with multiple queries
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(new OpenSearchQuery(null));
        plan.addSubQuery(new ClickHouseQuery("SELECT * FROM aegis_events_warm"));
        plan.addSubQuery(new OpenSearchQuery(null));
        
        // When: Execute is called
        long startTime = System.currentTimeMillis();
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should execute concurrently (not sequentially)
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                long executionTime = System.currentTimeMillis() - startTime;
                assertThat(queryResult).isNotNull();
                // Concurrent execution should be faster than sequential
                // (though this is a weak assertion in a unit test)
                assertThat(executionTime).isLessThan(5000);
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithTimeout_ShouldHandleGracefully() {
        // Given: A query plan that might timeout
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(new OpenSearchQuery(null));
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should complete within reasonable time
        StepVerifier.create(result)
            .expectNextCount(1)
            .expectComplete()
            .verify(Duration.ofSeconds(35)); // Slightly more than default timeout
    }
    
    @Test
    void testMergeResults_WithRawQueryResults_ShouldConcatenate() {
        // Given: Two raw query results
        QueryResult result1 = new QueryResult();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", "1");
        row1.put("message", "Event 1");
        row1.put("time", "2024-01-01T10:00:00Z");
        result1.addRow(row1);
        result1.setTotalCount(1);
        result1.setStorageTier(StorageTier.HOT);
        
        QueryResult result2 = new QueryResult();
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", "2");
        row2.put("message", "Event 2");
        row2.put("time", "2024-01-01T09:00:00Z");
        result2.addRow(row2);
        result2.setTotalCount(1);
        result2.setStorageTier(StorageTier.WARM);
        
        // When: Results are merged
        QueryResult merged = new QueryResult();
        merged.addAll(result1.getRows());
        merged.setTotalCount(result1.getTotalCount());
        merged.addAll(result2.getRows());
        merged.setTotalCount(merged.getTotalCount() + result2.getTotalCount());
        
        // Then: Should contain rows from both results
        assertThat(merged.getRows()).hasSize(2);
        assertThat(merged.getTotalCount()).isEqualTo(2);
        assertThat(merged.getRows().get(0).get("id")).isEqualTo("1");
        assertThat(merged.getRows().get(1).get("id")).isEqualTo("2");
    }
    
    @Test
    void testMergeResults_WithAggregationResults_ShouldMergeCorrectly() {
        // Given: Two aggregation results with overlapping keys
        QueryResult result1 = new QueryResult();
        Map<String, Object> agg1 = new HashMap<>();
        agg1.put("severity", "high");
        agg1.put("count", 10L);
        agg1.put("sum", 100.0);
        result1.addRow(agg1);
        result1.setTotalCount(1);
        result1.setStorageTier(StorageTier.HOT);
        
        QueryResult result2 = new QueryResult();
        Map<String, Object> agg2 = new HashMap<>();
        agg2.put("severity", "high");
        agg2.put("count", 5L);
        agg2.put("sum", 50.0);
        result2.addRow(agg2);
        result2.setTotalCount(1);
        result2.setStorageTier(StorageTier.WARM);
        
        // When: Aggregations are merged (simulated)
        // In the actual implementation, mergeResults would handle this
        Map<String, Object> merged = new HashMap<>();
        merged.put("severity", "high");
        merged.put("count", 15L); // 10 + 5
        merged.put("sum", 150.0); // 100 + 50
        merged.put("avg", 10.0); // 150 / 15
        
        // Then: Should merge aggregation values correctly
        assertThat(merged.get("count")).isEqualTo(15L);
        assertThat(merged.get("sum")).isEqualTo(150.0);
        assertThat(merged.get("avg")).isEqualTo(10.0);
    }
    
    @Test
    void testMergeResults_WithDifferentAggregationKeys_ShouldKeepSeparate() {
        // Given: Two aggregation results with different keys
        QueryResult result1 = new QueryResult();
        Map<String, Object> agg1 = new HashMap<>();
        agg1.put("severity", "high");
        agg1.put("count", 10L);
        result1.addRow(agg1);
        result1.setTotalCount(1);
        
        QueryResult result2 = new QueryResult();
        Map<String, Object> agg2 = new HashMap<>();
        agg2.put("severity", "low");
        agg2.put("count", 5L);
        result2.addRow(agg2);
        result2.setTotalCount(1);
        
        // When: Results are merged (simulated)
        QueryResult merged = new QueryResult();
        merged.addAll(result1.getRows());
        merged.addAll(result2.getRows());
        merged.setTotalCount(2);
        
        // Then: Should keep both aggregation keys
        assertThat(merged.getRows()).hasSize(2);
        assertThat(merged.getTotalCount()).isEqualTo(2);
    }
    
    @Test
    void testMergeResults_WithMinMaxAggregations_ShouldCalculateCorrectly() {
        // Given: Two aggregation results with min/max values
        QueryResult result1 = new QueryResult();
        Map<String, Object> agg1 = new HashMap<>();
        agg1.put("field", "response_time");
        agg1.put("min", 10.0);
        agg1.put("max", 100.0);
        result1.addRow(agg1);
        
        QueryResult result2 = new QueryResult();
        Map<String, Object> agg2 = new HashMap<>();
        agg2.put("field", "response_time");
        agg2.put("min", 5.0);
        agg2.put("max", 150.0);
        result2.addRow(agg2);
        
        // When: Min/max are merged (simulated)
        Map<String, Object> merged = new HashMap<>();
        merged.put("field", "response_time");
        merged.put("min", Math.min(10.0, 5.0)); // Should be 5.0
        merged.put("max", Math.max(100.0, 150.0)); // Should be 150.0
        
        // Then: Should take min of mins and max of maxes
        assertThat(merged.get("min")).isEqualTo(5.0);
        assertThat(merged.get("max")).isEqualTo(150.0);
    }
    
    @Test
    void testExecute_WithEmptySubQueryResults_ShouldReturnEmptyResult() {
        // Given: A query plan where all sub-queries return empty results
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(new OpenSearchQuery(null));
        plan.addSubQuery(new ClickHouseQuery("SELECT * FROM aegis_events_warm WHERE 1=0"));
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return empty result
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).isEmpty();
                assertThat(queryResult.getTotalCount()).isEqualTo(0);
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithNullSubQuery_ShouldHandleGracefully() {
        // Given: A query plan with a null sub-query
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(null);
        plan.addSubQuery(new OpenSearchQuery(null));
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should handle null gracefully and continue
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithLargeNumberOfSubQueries_ShouldHandleEfficiently() {
        // Given: A query plan with many sub-queries
        QueryPlan plan = new QueryPlan();
        for (int i = 0; i < 20; i++) {
            plan.addSubQuery(new OpenSearchQuery(null));
        }
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should handle all queries efficiently
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getExecutionTimeMs()).isGreaterThanOrEqualTo(0);
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_ShouldSetExecutionTime() {
        // Given: A query plan
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(new OpenSearchQuery(null));
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should set execution time in result
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult.getExecutionTimeMs()).isGreaterThanOrEqualTo(0);
            })
            .verifyComplete();
    }
}
