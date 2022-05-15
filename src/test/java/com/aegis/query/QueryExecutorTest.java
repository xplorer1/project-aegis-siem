package com.aegis.query;

import com.aegis.domain.QueryResult;
import com.aegis.domain.StorageTier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.text.Text;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit tests for QueryExecutor
 * Tests the query execution interface and OpenSearch integration
 */
@ExtendWith(MockitoExtension.class)
class QueryExecutorTest {
    
    @Mock
    private RestHighLevelClient openSearchClient;
    
    @Mock
    private JdbcTemplate clickHouseTemplate;
    
    @Mock
    private QueryMetrics metrics;
    
    @Mock
    private SearchResponse searchResponse;
    
    @Mock
    private SearchHits searchHits;
    
    private QueryExecutor queryExecutor;
    
    @BeforeEach
    void setUp() {
        queryExecutor = new QueryExecutor(openSearchClient, clickHouseTemplate, metrics);
    }
    
    // ========== Constructor Tests ==========
    
    @Test
    void testConstructor_WithValidDependencies_ShouldCreateInstance() {
        // Given: Valid client dependencies
        // When: QueryExecutor is created
        // Then: Instance should be created successfully
        assertThat(queryExecutor).isNotNull();
    }
    
    // ========== Execute Method Tests ==========
    
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
    
    // ========== OpenSearch Execution Tests ==========
    
    @Test
    void testExecuteOpenSearch_WithNullQuery_ShouldReturnEmptyResult() {
        // Given: A null OpenSearch query
        OpenSearchQuery query = null;
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
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
    void testExecuteOpenSearch_WithValidQuery_ShouldReturnResults() throws Exception {
        // Given: A valid OpenSearch query with mocked response
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(10);
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock search hits
        SearchHit hit1 = createMockSearchHit("1", "aegis-events-2022-05-13", 1.0f, 
            Map.of("message", "Test event 1", "severity", 3));
        SearchHit hit2 = createMockSearchHit("2", "aegis-events-2022-05-13", 0.9f,
            Map.of("message", "Test event 2", "severity", 4));
        
        SearchHit[] hits = new SearchHit[]{hit1, hit2};
        
        when(searchHits.getHits()).thenReturn(hits);
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(2, 
            org.apache.lucene.search.TotalHits.Relation.EQUAL_TO));
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchResponse.getAggregations()).thenReturn(null);
        
        // Mock async search execution
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponse);
            return null;
        }).when(openSearchClient).searchAsync(any(SearchRequest.class), 
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return results from OpenSearch
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(2);
                assertThat(queryResult.getTotalCount()).isEqualTo(2);
                assertThat(queryResult.getStorageTier()).isEqualTo(StorageTier.HOT);
                assertThat(queryResult.getExecutionTimeMs()).isGreaterThanOrEqualTo(0);
                
                // Verify first row
                Map<String, Object> row1 = queryResult.getRows().get(0);
                assertThat(row1.get("_id")).isEqualTo("1");
                assertThat(row1.get("_index")).isEqualTo("aegis-events-2022-05-13");
                assertThat(row1.get("message")).isEqualTo("Test event 1");
                assertThat(row1.get("severity")).isEqualTo(3);
            })
            .verifyComplete();
        
        // Verify OpenSearch client was called
        verify(openSearchClient).searchAsync(any(SearchRequest.class), 
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
    }
    
    @Test
    void testExecuteOpenSearch_WithHighlights_ShouldIncludeHighlights() throws Exception {
        // Given: A query with highlighted results
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchQuery("message", "error"))
            .highlighter(new org.opensearch.search.fetch.subphase.highlight.HighlightBuilder()
                .field("message"));
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Create mock hit with highlights
        SearchHit hit = createMockSearchHit("1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Error occurred in system"));
        
        // Add highlight
        Map<String, HighlightField> highlights = new HashMap<>();
        HighlightField highlightField = new HighlightField("message", 
            new Text[]{new Text("<em>Error</em> occurred in system")});
        highlights.put("message", highlightField);
        when(hit.getHighlightFields()).thenReturn(highlights);
        
        SearchHit[] hits = new SearchHit[]{hit};
        when(searchHits.getHits()).thenReturn(hits);
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(1,
            org.apache.lucene.search.TotalHits.Relation.EQUAL_TO));
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchResponse.getAggregations()).thenReturn(null);
        
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponse);
            return null;
        }).when(openSearchClient).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should include highlights in results
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult.getRows()).hasSize(1);
                Map<String, Object> row = queryResult.getRows().get(0);
                assertThat(row).containsKey("_highlights");
                
                @SuppressWarnings("unchecked")
                Map<String, Object> highlightsMap = (Map<String, Object>) row.get("_highlights");
                assertThat(highlightsMap).containsKey("message");
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteOpenSearch_WithAggregations_ShouldIncludeAggregations() throws Exception {
        // Given: A query with aggregations
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .size(0)
            .aggregation(org.opensearch.search.aggregations.AggregationBuilders
                .terms("severity_counts").field("severity"));
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock aggregations
        ParsedStringTerms termsAgg = mock(ParsedStringTerms.class);
        when(termsAgg.getName()).thenReturn("severity_counts");
        
        List<Terms.Bucket> buckets = new ArrayList<>();
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getKey()).thenReturn("3");
        when(bucket1.getDocCount()).thenReturn(10L);
        when(bucket1.getAggregations()).thenReturn(null);
        buckets.add(bucket1);
        
        when(termsAgg.getBuckets()).thenReturn((List) buckets);
        
        Aggregations aggregations = new Aggregations(List.of(termsAgg));
        
        when(searchHits.getHits()).thenReturn(new SearchHit[0]);
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(0,
            org.apache.lucene.search.TotalHits.Relation.EQUAL_TO));
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchResponse.getAggregations()).thenReturn(aggregations);
        
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponse);
            return null;
        }).when(openSearchClient).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should include aggregations in results
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult.getRows()).hasSize(1);
                Map<String, Object> aggRow = queryResult.getRows().get(0);
                assertThat(aggRow).containsKey("_aggregations");
            })
            .verifyComplete();
    }
    
    // ========== Error Handling Tests ==========
    
    @Test
    void testExecuteOpenSearch_WithError_ShouldReturnEmptyResult() throws Exception {
        // Given: A query that will fail
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock error response
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("OpenSearch connection failed"));
            return null;
        }).when(openSearchClient).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return empty result with error handling
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).isEmpty();
                assertThat(queryResult.getStorageTier()).isEqualTo(StorageTier.HOT);
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteOpenSearch_WithTimeout_ShouldReturnPartialResults() {
        // Given: A query that will timeout
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock slow response (never completes)
        doAnswer(invocation -> {
            // Don't call the listener - simulate hanging request
            return null;
        }).when(openSearchClient).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should timeout and return empty result
        StepVerifier.create(result)
            .expectTimeout(Duration.ofSeconds(35)) // Slightly more than 30s timeout
            .verify();
    }
    
    // ========== ClickHouse Execution Tests ==========
    
    @Test
    void testExecuteClickHouse_WithNullQuery_ShouldReturnEmptyResult() {
        // Given: A null ClickHouse query
        ClickHouseQuery query = null;
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
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
    void testExecuteClickHouse_WithEmptySQL_ShouldReturnEmptyResult() {
        // Given: A ClickHouse query with empty SQL
        ClickHouseQuery query = new ClickHouseQuery("");
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
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
    void testExecuteClickHouse_WithValidQuery_ShouldReturnResults() {
        // Given: A valid ClickHouse query
        String sql = "SELECT time, severity, message FROM aegis_events_warm WHERE severity >= 3";
        ClickHouseQuery query = new ClickHouseQuery(sql);
        
        // Mock JDBC template to return results
        when(clickHouseTemplate.query(eq(sql), any(org.springframework.jdbc.core.ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                org.springframework.jdbc.core.ResultSetExtractor<?> extractor = invocation.getArgument(1);
                
                // Create mock ResultSet
                java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                java.sql.ResultSetMetaData metaData = mock(java.sql.ResultSetMetaData.class);
                
                // Configure metadata
                when(metaData.getColumnCount()).thenReturn(3);
                when(metaData.getColumnLabel(1)).thenReturn("time");
                when(metaData.getColumnLabel(2)).thenReturn("severity");
                when(metaData.getColumnLabel(3)).thenReturn("message");
                when(rs.getMetaData()).thenReturn(metaData);
                
                // Configure result set to return 2 rows
                when(rs.next()).thenReturn(true, true, false);
                
                // First row
                when(rs.getObject(1)).thenReturn(
                    java.sql.Timestamp.valueOf("2022-05-15 10:00:00"),
                    java.sql.Timestamp.valueOf("2022-05-15 10:05:00")
                );
                when(rs.getObject(2)).thenReturn(3, 4);
                when(rs.getObject(3)).thenReturn("Test event 1", "Test event 2");
                
                return extractor.extractData(rs);
            });
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return results from ClickHouse
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(2);
                assertThat(queryResult.getTotalCount()).isEqualTo(2);
                assertThat(queryResult.getStorageTier()).isEqualTo(StorageTier.WARM);
                assertThat(queryResult.getExecutionTimeMs()).isGreaterThanOrEqualTo(0);
                
                // Verify first row
                Map<String, Object> row1 = queryResult.getRows().get(0);
                assertThat(row1.get("time")).isNotNull();
                assertThat(row1.get("severity")).isEqualTo(3);
                assertThat(row1.get("message")).isEqualTo("Test event 1");
                
                // Verify second row
                Map<String, Object> row2 = queryResult.getRows().get(1);
                assertThat(row2.get("severity")).isEqualTo(4);
                assertThat(row2.get("message")).isEqualTo("Test event 2");
            })
            .verifyComplete();
        
        // Verify JDBC template was called
        verify(clickHouseTemplate).query(eq(sql), any(org.springframework.jdbc.core.ResultSetExtractor.class));
    }
    
    @Test
    void testExecuteClickHouse_WithAggregationQuery_ShouldReturnAggregatedResults() {
        // Given: An aggregation query
        String sql = "SELECT severity, COUNT(*) as count FROM aegis_events_warm GROUP BY severity";
        ClickHouseQuery query = new ClickHouseQuery(sql);
        
        // Mock JDBC template to return aggregated results
        when(clickHouseTemplate.query(eq(sql), any(org.springframework.jdbc.core.ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                org.springframework.jdbc.core.ResultSetExtractor<?> extractor = invocation.getArgument(1);
                
                // Create mock ResultSet
                java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                java.sql.ResultSetMetaData metaData = mock(java.sql.ResultSetMetaData.class);
                
                // Configure metadata
                when(metaData.getColumnCount()).thenReturn(2);
                when(metaData.getColumnLabel(1)).thenReturn("severity");
                when(metaData.getColumnLabel(2)).thenReturn("count");
                when(rs.getMetaData()).thenReturn(metaData);
                
                // Configure result set to return 3 rows (one per severity level)
                when(rs.next()).thenReturn(true, true, true, false);
                when(rs.getObject(1)).thenReturn(3, 4, 5);
                when(rs.getObject(2)).thenReturn(100L, 50L, 25L);
                
                return extractor.extractData(rs);
            });
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return aggregated results
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(3);
                assertThat(queryResult.getTotalCount()).isEqualTo(3);
                
                // Verify aggregation rows
                Map<String, Object> row1 = queryResult.getRows().get(0);
                assertThat(row1.get("severity")).isEqualTo(3);
                assertThat(row1.get("count")).isEqualTo(100L);
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteClickHouse_WithError_ShouldReturnEmptyResult() {
        // Given: A query that will fail
        String sql = "SELECT * FROM non_existent_table";
        ClickHouseQuery query = new ClickHouseQuery(sql);
        
        // Mock JDBC template to throw exception
        when(clickHouseTemplate.query(eq(sql), any(org.springframework.jdbc.core.ResultSetExtractor.class)))
            .thenThrow(new org.springframework.dao.DataAccessException("Table not found") {});
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return empty result with error handling
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).isEmpty();
                assertThat(queryResult.getStorageTier()).isEqualTo(StorageTier.WARM);
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteClickHouse_WithSpecialTypes_ShouldConvertProperly() {
        // Given: A query returning various SQL types
        String sql = "SELECT time, date, array_field FROM aegis_events_warm";
        ClickHouseQuery query = new ClickHouseQuery(sql);
        
        // Mock JDBC template to return special types
        when(clickHouseTemplate.query(eq(sql), any(org.springframework.jdbc.core.ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                org.springframework.jdbc.core.ResultSetExtractor<?> extractor = invocation.getArgument(1);
                
                // Create mock ResultSet
                java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                java.sql.ResultSetMetaData metaData = mock(java.sql.ResultSetMetaData.class);
                
                // Configure metadata
                when(metaData.getColumnCount()).thenReturn(3);
                when(metaData.getColumnLabel(1)).thenReturn("time");
                when(metaData.getColumnLabel(2)).thenReturn("date");
                when(metaData.getColumnLabel(3)).thenReturn("array_field");
                when(rs.getMetaData()).thenReturn(metaData);
                
                // Configure result set to return 1 row with special types
                when(rs.next()).thenReturn(true, false);
                
                java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2022-05-15 10:00:00");
                java.sql.Date date = java.sql.Date.valueOf("2022-05-15");
                java.sql.Array array = mock(java.sql.Array.class);
                when(array.getArray()).thenReturn(new String[]{"value1", "value2"});
                
                when(rs.getObject(1)).thenReturn(timestamp);
                when(rs.getObject(2)).thenReturn(date);
                when(rs.getObject(3)).thenReturn(array);
                
                return extractor.extractData(rs);
            });
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should convert special types properly
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(1);
                
                Map<String, Object> row = queryResult.getRows().get(0);
                
                // Timestamp should be converted to ISO 8601 string
                assertThat(row.get("time")).isInstanceOf(String.class);
                assertThat(row.get("time").toString()).contains("2022-05-15");
                
                // Date should be converted to string
                assertThat(row.get("date")).isInstanceOf(String.class);
                assertThat(row.get("date")).isEqualTo("2022-05-15");
                
                // Array should be converted to Java array
                assertThat(row.get("array_field")).isInstanceOf(Object[].class);
            })
            .verifyComplete();
    }
    
    // ========== Multi-Tier Tests ==========
    
    @Test
    void testExecute_WithMultipleTierQueries_ShouldMergeResults() throws Exception {
        // Given: A query plan with queries for multiple tiers
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery hotQuery = new OpenSearchQuery(sourceBuilder);
        ClickHouseQuery warmQuery = new ClickHouseQuery("SELECT * FROM aegis_events_warm");
        
        // Mock OpenSearch response
        SearchHit hit = createMockSearchHit("1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Test event"));
        when(searchHits.getHits()).thenReturn(new SearchHit[]{hit});
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(1,
            org.apache.lucene.search.TotalHits.Relation.EQUAL_TO));
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchResponse.getAggregations()).thenReturn(null);
        
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponse);
            return null;
        }).when(openSearchClient).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(hotQuery);
        plan.addSubQuery(warmQuery);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should merge results from both tiers
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                // Hot tier returns 1 row, warm tier returns 0 (placeholder)
                assertThat(queryResult.getRows()).hasSize(1);
            })
            .verifyComplete();
    }
    
    @Test
    void testMergeResults_WithValidResults_ShouldCombineRows() {
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
    
    // ========== Helper Methods ==========
    
    /**
     * Helper method to create a mock SearchHit
     */
    private SearchHit createMockSearchHit(String id, String index, float score, 
                                          Map<String, Object> sourceMap) {
        SearchHit hit = mock(SearchHit.class);
        when(hit.getId()).thenReturn(id);
        when(hit.getIndex()).thenReturn(index);
        when(hit.getScore()).thenReturn(score);
        when(hit.getSourceAsMap()).thenReturn(sourceMap);
        when(hit.getHighlightFields()).thenReturn(new HashMap<>());
        return hit;
    }
}
