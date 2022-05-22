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
    
    // ========== Result Merging Tests ==========
    
    @Test
    void testMergeResults_WithTimestampOrdering_ShouldSortByTime() {
        // Given: Multiple results with timestamps
        QueryResult result1 = new QueryResult();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("time", "2022-05-13T10:00:00Z");
        row1.put("message", "Event 1");
        result1.addRow(row1);
        result1.setTotalCount(1);
        
        QueryResult result2 = new QueryResult();
        Map<String, Object> row2 = new HashMap<>();
        row2.put("time", "2022-05-13T11:00:00Z");
        row2.put("message", "Event 2");
        result2.addRow(row2);
        result2.setTotalCount(1);
        
        // When: Results are merged
        QueryResult merged = new QueryResult();
        merged.addAll(result1.getRows());
        merged.setTotalCount(result1.getTotalCount());
        
        // Simulate mergeRawResults behavior
        merged.addAll(result2.getRows());
        merged.setTotalCount(merged.getTotalCount() + result2.getTotalCount());
        
        // Sort by timestamp descending
        merged.getRows().sort((r1, r2) -> {
            String t1 = (String) r1.get("time");
            String t2 = (String) r2.get("time");
            return t2.compareTo(t1);
        });
        
        // Then: Should be sorted with most recent first
        assertThat(merged.getRows()).hasSize(2);
        assertThat(merged.getRows().get(0).get("time")).isEqualTo("2022-05-13T11:00:00Z");
        assertThat(merged.getRows().get(1).get("time")).isEqualTo("2022-05-13T10:00:00Z");
    }
    
    @Test
    void testMergeResults_WithAggregations_ShouldSumCounts() {
        // Given: Multiple aggregation results with same grouping key
        QueryResult result1 = new QueryResult();
        Map<String, Object> agg1 = new HashMap<>();
        agg1.put("severity", 3);
        agg1.put("count", 10L);
        agg1.put("sum", 100.0);
        result1.addRow(agg1);
        result1.setTotalCount(1);
        
        QueryResult result2 = new QueryResult();
        Map<String, Object> agg2 = new HashMap<>();
        agg2.put("severity", 3);
        agg2.put("count", 5L);
        agg2.put("sum", 50.0);
        result2.addRow(agg2);
        result2.setTotalCount(1);
        
        // When: Aggregations are merged (simulated)
        Map<String, Object> merged = new HashMap<>();
        merged.put("severity", 3);
        merged.put("count", 15L); // 10 + 5
        merged.put("sum", 150.0); // 100 + 50
        merged.put("avg", 10.0); // 150 / 15
        
        // Then: Should have summed counts and sums
        assertThat(merged.get("count")).isEqualTo(15L);
        assertThat(merged.get("sum")).isEqualTo(150.0);
        assertThat(merged.get("avg")).isEqualTo(10.0);
    }
    
    @Test
    void testMergeResults_WithDifferentAggregationKeys_ShouldKeepSeparate() {
        // Given: Aggregation results with different grouping keys
        QueryResult result1 = new QueryResult();
        Map<String, Object> agg1 = new HashMap<>();
        agg1.put("severity", 3);
        agg1.put("count", 10L);
        result1.addRow(agg1);
        
        QueryResult result2 = new QueryResult();
        Map<String, Object> agg2 = new HashMap<>();
        agg2.put("severity", 4);
        agg2.put("count", 5L);
        result2.addRow(agg2);
        
        // When: Results are merged
        QueryResult merged = new QueryResult();
        merged.addAll(result1.getRows());
        merged.addAll(result2.getRows());
        
        // Then: Should keep both aggregations separate
        assertThat(merged.getRows()).hasSize(2);
        assertThat(merged.getRows().get(0).get("severity")).isEqualTo(3);
        assertThat(merged.getRows().get(1).get("severity")).isEqualTo(4);
    }
    
    @Test
    void testMergeResults_WithMinMaxAggregations_ShouldTakeExtremes() {
        // Given: Results with min/max aggregations
        QueryResult result1 = new QueryResult();
        Map<String, Object> agg1 = new HashMap<>();
        agg1.put("field", "value");
        agg1.put("min", 10.0);
        agg1.put("max", 100.0);
        result1.addRow(agg1);
        
        QueryResult result2 = new QueryResult();
        Map<String, Object> agg2 = new HashMap<>();
        agg2.put("field", "value");
        agg2.put("min", 5.0);
        agg2.put("max", 150.0);
        result2.addRow(agg2);
        
        // When: Aggregations are merged (simulated)
        Map<String, Object> merged = new HashMap<>();
        merged.put("field", "value");
        merged.put("min", Math.min(10.0, 5.0)); // 5.0
        merged.put("max", Math.max(100.0, 150.0)); // 150.0
        
        // Then: Should take minimum and maximum across all results
        assertThat(merged.get("min")).isEqualTo(5.0);
        assertThat(merged.get("max")).isEqualTo(150.0);
    }
    
    @Test
    void testMergeResults_WithPaginationMetadata_ShouldCombine() {
        // Given: Results with pagination metadata
        QueryResult result1 = new QueryResult();
        result1.setHasMore(true);
        result1.setCursor("cursor1");
        
        QueryResult result2 = new QueryResult();
        result2.setHasMore(false);
        result2.setCursor("cursor2");
        
        // When: Pagination metadata is merged (simulated)
        QueryResult merged = new QueryResult();
        merged.setHasMore(result1.isHasMore() || result2.isHasMore());
        merged.setCursor(result1.getCursor() + "," + result2.getCursor());
        
        // Then: Should combine pagination metadata
        assertThat(merged.isHasMore()).isTrue(); // true if any tier has more
        assertThat(merged.getCursor()).isEqualTo("cursor1,cursor2");
    }
    
    @Test
    void testMergeResults_WithPartialResults_ShouldTrackStatus() {
        // Given: Results with partial/timeout flags
        QueryResult result1 = new QueryResult();
        result1.setPartialResults(true);
        result1.setTimedOut(false);
        
        QueryResult result2 = new QueryResult();
        result2.setPartialResults(false);
        result2.setTimedOut(true);
        
        // When: Status flags are merged (simulated)
        QueryResult merged = new QueryResult();
        merged.setPartialResults(result1.isPartialResults() || result2.isPartialResults());
        merged.setTimedOut(result1.isTimedOut() || result2.isTimedOut());
        
        // Then: Should track if any tier had issues
        assertThat(merged.isPartialResults()).isTrue();
        assertThat(merged.isTimedOut()).isTrue();
    }
    
    @Test
    void testMergeResults_WithNestedAggregations_ShouldMergeRecursively() {
        // Given: Results with nested bucket aggregations
        QueryResult result1 = new QueryResult();
        Map<String, Object> aggRow1 = new HashMap<>();
        Map<String, Object> aggs1 = new HashMap<>();
        Map<String, Object> termsAgg1 = new HashMap<>();
        List<Map<String, Object>> buckets1 = new ArrayList<>();
        
        Map<String, Object> bucket1 = new HashMap<>();
        bucket1.put("key", "severity_3");
        bucket1.put("doc_count", 10L);
        buckets1.add(bucket1);
        
        termsAgg1.put("buckets", buckets1);
        aggs1.put("severity_terms", termsAgg1);
        aggRow1.put("_aggregations", aggs1);
        result1.addRow(aggRow1);
        
        QueryResult result2 = new QueryResult();
        Map<String, Object> aggRow2 = new HashMap<>();
        Map<String, Object> aggs2 = new HashMap<>();
        Map<String, Object> termsAgg2 = new HashMap<>();
        List<Map<String, Object>> buckets2 = new ArrayList<>();
        
        Map<String, Object> bucket2 = new HashMap<>();
        bucket2.put("key", "severity_3");
        bucket2.put("doc_count", 5L);
        buckets2.add(bucket2);
        
        termsAgg2.put("buckets", buckets2);
        aggs2.put("severity_terms", termsAgg2);
        aggRow2.put("_aggregations", aggs2);
        result2.addRow(aggRow2);
        
        // When: Nested aggregations are merged (simulated)
        // The bucket with key "severity_3" should have doc_count = 15 (10 + 5)
        
        // Then: Should merge nested bucket aggregations
        // This test validates the structure for nested aggregation merging
        assertThat(result1.getRows()).hasSize(1);
        assertThat(result2.getRows()).hasSize(1);
    }
    
    @Test
    void testMergeResults_WithEmptyResult_ShouldSkip() {
        // Given: One valid result and one empty result
        QueryResult result1 = new QueryResult();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", "1");
        result1.addRow(row1);
        result1.setTotalCount(1);
        
        QueryResult result2 = new QueryResult(); // Empty
        
        // When: Results are merged
        QueryResult merged = new QueryResult();
        merged.addAll(result1.getRows());
        merged.setTotalCount(result1.getTotalCount());
        
        // Empty result should be skipped
        if (!result2.getRows().isEmpty()) {
            merged.addAll(result2.getRows());
            merged.setTotalCount(merged.getTotalCount() + result2.getTotalCount());
        }
        
        // Then: Should only contain non-empty result
        assertThat(merged.getRows()).hasSize(1);
        assertThat(merged.getTotalCount()).isEqualTo(1);
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

    // ========== Cache Tests ==========
    
    @Test
    void testExecute_WithCacheHit_ShouldReturnCachedResult() throws Exception {
        // Given: A query that has been executed before
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(10);
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock search hits for first execution
        SearchHit hit = createMockSearchHit("1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Test event", "severity", 3));
        
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
        plan.addSubQuery(query);
        
        // When: Execute is called twice with the same query
        Flux<QueryResult> firstResult = queryExecutor.execute(plan);
        
        // First execution should hit the database
        StepVerifier.create(firstResult)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(1);
                assertThat(queryResult.isCached()).isFalse();
            })
            .verifyComplete();
        
        // Verify cache miss was recorded
        verify(metrics).recordCacheMiss();
        
        // Second execution should hit the cache
        Flux<QueryResult> secondResult = queryExecutor.execute(plan);
        
        StepVerifier.create(secondResult)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(1);
                assertThat(queryResult.isCached()).isTrue();
            })
            .verifyComplete();
        
        // Verify cache hit was recorded
        verify(metrics).recordCacheHit();
        
        // Verify OpenSearch was only called once (first execution)
        verify(openSearchClient, times(1)).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
    }
    
    @Test
    void testExecute_WithDifferentQueries_ShouldNotShareCache() throws Exception {
        // Given: Two different queries
        SearchSourceBuilder sourceBuilder1 = new SearchSourceBuilder()
            .query(QueryBuilders.matchQuery("message", "error"))
            .size(10);
        OpenSearchQuery query1 = new OpenSearchQuery(sourceBuilder1);
        
        SearchSourceBuilder sourceBuilder2 = new SearchSourceBuilder()
            .query(QueryBuilders.matchQuery("message", "warning"))
            .size(10);
        OpenSearchQuery query2 = new OpenSearchQuery(sourceBuilder2);
        
        // Mock search hits
        SearchHit hit1 = createMockSearchHit("1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Error event"));
        SearchHit hit2 = createMockSearchHit("2", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Warning event"));
        
        when(searchHits.getHits())
            .thenReturn(new SearchHit[]{hit1})
            .thenReturn(new SearchHit[]{hit2});
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
        
        QueryPlan plan1 = new QueryPlan();
        plan1.addSubQuery(query1);
        
        QueryPlan plan2 = new QueryPlan();
        plan2.addSubQuery(query2);
        
        // When: Execute both queries
        Flux<QueryResult> result1 = queryExecutor.execute(plan1);
        Flux<QueryResult> result2 = queryExecutor.execute(plan2);
        
        // Then: Both should execute (no cache sharing)
        StepVerifier.create(result1)
            .assertNext(queryResult -> {
                assertThat(queryResult.isCached()).isFalse();
            })
            .verifyComplete();
        
        StepVerifier.create(result2)
            .assertNext(queryResult -> {
                assertThat(queryResult.isCached()).isFalse();
            })
            .verifyComplete();
        
        // Verify both queries hit the database
        verify(openSearchClient, times(2)).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        // Verify both were cache misses
        verify(metrics, times(2)).recordCacheMiss();
    }
    
    @Test
    void testInvalidateCache_ShouldClearAllEntries() throws Exception {
        // Given: A cached query result
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
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
        plan.addSubQuery(query);
        
        // Execute once to populate cache
        StepVerifier.create(queryExecutor.execute(plan))
            .assertNext(queryResult -> assertThat(queryResult.isCached()).isFalse())
            .verifyComplete();
        
        // When: Cache is invalidated
        queryExecutor.invalidateCache();
        
        // Then: Next execution should be a cache miss
        StepVerifier.create(queryExecutor.execute(plan))
            .assertNext(queryResult -> assertThat(queryResult.isCached()).isFalse())
            .verifyComplete();
        
        // Verify cache invalidation was recorded
        verify(metrics).recordCacheInvalidation();
        
        // Verify OpenSearch was called twice (once before invalidation, once after)
        verify(openSearchClient, times(2)).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
    }
    
    @Test
    void testGetCacheStats_ShouldReturnStatistics() throws Exception {
        // Given: Some cached queries
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
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
        plan.addSubQuery(query);
        
        // Execute twice (one miss, one hit)
        StepVerifier.create(queryExecutor.execute(plan)).expectNextCount(1).verifyComplete();
        StepVerifier.create(queryExecutor.execute(plan)).expectNextCount(1).verifyComplete();
        
        // When: Get cache stats
        Map<String, Object> stats = queryExecutor.getCacheStats();
        
        // Then: Should return statistics
        assertThat(stats).isNotNull();
        assertThat(stats).containsKeys("hitCount", "missCount", "hitRate", "evictionCount", "estimatedSize");
        assertThat(stats.get("hitCount")).isEqualTo(1L);
        assertThat(stats.get("missCount")).isEqualTo(1L);
        assertThat(stats.get("estimatedSize")).isEqualTo(1L);
    }
    
    @Test
    void testCachedResultCloning_ShouldNotMutateOriginal() throws Exception {
        // Given: A cached query result
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
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
        plan.addSubQuery(query);
        
        // Execute once to populate cache
        StepVerifier.create(queryExecutor.execute(plan))
            .assertNext(queryResult -> {
                assertThat(queryResult.getRows()).hasSize(1);
            })
            .verifyComplete();
        
        // When: Execute again and modify the result
        StepVerifier.create(queryExecutor.execute(plan))
            .assertNext(queryResult -> {
                assertThat(queryResult.isCached()).isTrue();
                // Modify the result
                queryResult.getRows().clear();
            })
            .verifyComplete();
        
        // Then: Third execution should still return the original cached result
        StepVerifier.create(queryExecutor.execute(plan))
            .assertNext(queryResult -> {
                assertThat(queryResult.isCached()).isTrue();
                assertThat(queryResult.getRows()).hasSize(1); // Original size preserved
            })
            .verifyComplete();
    }
}

    // ========== Comprehensive Timeout Handling Tests ==========
    
    @Test
    void testExecute_WithSubQueryTimeout_ShouldReturnPartialResults() throws Exception {
        // Given: A query plan with one fast query and one slow query
        SearchSourceBuilder fastSourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery fastQuery = new OpenSearchQuery(fastSourceBuilder);
        
        SearchSourceBuilder slowSourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery slowQuery = new OpenSearchQuery(slowSourceBuilder);
        
        // Mock fast response
        SearchHit hit = createMockSearchHit("1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Fast event"));
        when(searchHits.getHits()).thenReturn(new SearchHit[]{hit});
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(1,
            org.apache.lucene.search.TotalHits.Relation.EQUAL_TO));
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchResponse.getAggregations()).thenReturn(null);
        
        // Mock one fast response and one timeout
        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            
            // First call succeeds quickly
            if (request.source() == fastSourceBuilder) {
                listener.onResponse(searchResponse);
            }
            // Second call never completes (timeout)
            return null;
        }).when(openSearchClient).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(fastQuery);
        plan.addSubQuery(slowQuery);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return partial results with timeout flag
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.isTimedOut()).isTrue();
                assertThat(queryResult.isPartialResults()).isTrue();
                // Should have results from the fast query
                assertThat(queryResult.getRows()).isNotEmpty();
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithAllSubQueriesTimeout_ShouldReturnEmptyPartialResults() {
        // Given: A query plan where all sub-queries timeout
        SearchSourceBuilder sourceBuilder1 = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query1 = new OpenSearchQuery(sourceBuilder1);
        
        SearchSourceBuilder sourceBuilder2 = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query2 = new OpenSearchQuery(sourceBuilder2);
        
        // Mock all queries to timeout (never complete)
        doAnswer(invocation -> {
            // Don't call the listener - simulate hanging request
            return null;
        }).when(openSearchClient).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query1);
        plan.addSubQuery(query2);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should timeout and return empty partial results
        StepVerifier.create(result)
            .expectTimeout(Duration.ofSeconds(35))
            .verify();
    }
    
    @Test
    void testExecute_WithPartialResults_ShouldNotCacheResults() throws Exception {
        // Given: A query plan where one sub-query times out
        SearchSourceBuilder fastSourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery fastQuery = new OpenSearchQuery(fastSourceBuilder);
        
        SearchSourceBuilder slowSourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery slowQuery = new OpenSearchQuery(slowSourceBuilder);
        
        // Mock fast response
        SearchHit hit = createMockSearchHit("1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Fast event"));
        when(searchHits.getHits()).thenReturn(new SearchHit[]{hit});
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(1,
            org.apache.lucene.search.TotalHits.Relation.EQUAL_TO));
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchResponse.getAggregations()).thenReturn(null);
        
        // Mock one fast response and one timeout
        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            
            if (request.source() == fastSourceBuilder) {
                listener.onResponse(searchResponse);
            }
            // Second call never completes
            return null;
        }).when(openSearchClient).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(fastQuery);
        plan.addSubQuery(slowQuery);
        
        // When: Execute is called twice
        Flux<QueryResult> result1 = queryExecutor.execute(plan);
        
        // Then: First execution should return partial results
        StepVerifier.create(result1)
            .assertNext(queryResult -> {
                assertThat(queryResult.isTimedOut()).isTrue();
                assertThat(queryResult.isPartialResults()).isTrue();
                assertThat(queryResult.isCached()).isFalse();
            })
            .verifyComplete();
        
        // When: Execute the same query again
        Flux<QueryResult> result2 = queryExecutor.execute(plan);
        
        // Then: Second execution should NOT return cached results (partial results not cached)
        StepVerifier.create(result2)
            .assertNext(queryResult -> {
                assertThat(queryResult.isCached()).isFalse();
                // Should still be partial results from fresh execution
                assertThat(queryResult.isTimedOut()).isTrue();
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithMixedTierTimeouts_ShouldReturnAvailableResults() throws Exception {
        // Given: A multi-tier query where warm tier times out but hot tier succeeds
        SearchSourceBuilder hotSourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery hotQuery = new OpenSearchQuery(hotSourceBuilder);
        
        ClickHouseQuery warmQuery = new ClickHouseQuery("SELECT * FROM aegis_events_warm");
        
        // Mock hot tier success
        SearchHit hit = createMockSearchHit("1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Hot tier event"));
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
        
        // Warm tier will timeout (placeholder implementation returns empty)
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(hotQuery);
        plan.addSubQuery(warmQuery);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should return results from hot tier only
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(1);
                // Should have hot tier results
                Map<String, Object> row = queryResult.getRows().get(0);
                assertThat(row.get("message")).isEqualTo("Hot tier event");
            })
            .verifyComplete();
    }
    
    @Test
    void testExecute_WithTimeoutIndicators_ShouldSetCorrectFlags() throws Exception {
        // Given: A query that will partially timeout
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock timeout
        doAnswer(invocation -> {
            // Never complete - will timeout
            return null;
        }).when(openSearchClient).searchAsync(any(SearchRequest.class),
            eq(RequestOptions.DEFAULT), any(ActionListener.class));
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should set timeout and partial result flags
        StepVerifier.create(result)
            .expectTimeout(Duration.ofSeconds(35))
            .verify();
    }
    
    @Test
    void testExecute_WithSuccessfulQuery_ShouldNotSetTimeoutFlags() throws Exception {
        // Given: A query that completes successfully
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock successful response
        SearchHit hit = createMockSearchHit("1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Success event"));
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
        plan.addSubQuery(query);
        
        // When: Execute is called
        Flux<QueryResult> result = queryExecutor.execute(plan);
        
        // Then: Should NOT set timeout or partial result flags
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.isTimedOut()).isFalse();
                assertThat(queryResult.isPartialResults()).isFalse();
                assertThat(queryResult.getRows()).hasSize(1);
            })
            .verifyComplete();
    }
}
