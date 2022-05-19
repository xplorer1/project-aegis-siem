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
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Comprehensive tests for QueryExecutor pagination functionality
 * Tests cursor-based pagination, page size handling, and large result sets
 */
@ExtendWith(MockitoExtension.class)
class QueryExecutorPaginationTest {
    
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
    
    // ========== Helper Methods ==========
    
    private SearchHit createMockSearchHit(String id, String index, float score, Map<String, Object> source) {
        SearchHit hit = mock(SearchHit.class);
        when(hit.getId()).thenReturn(id);
        when(hit.getIndex()).thenReturn(index);
        when(hit.getScore()).thenReturn(score);
        when(hit.getSourceAsMap()).thenReturn(source);
        when(hit.getHighlightFields()).thenReturn(new HashMap<>());
        return hit;
    }
    
    private String createCursor(String lastId, String lastTimestamp, int offset) {
        try {
            String cursorJson = String.format(
                "{\"lastId\":\"%s\",\"lastTimestamp\":\"%s\",\"offset\":%d}",
                lastId, lastTimestamp, offset
            );
            return Base64.getEncoder().encodeToString(cursorJson.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create cursor", e);
        }
    }
    
    // ========== Basic Pagination Tests ==========
    
    @Test
    void testExecuteWithPagination_WithNullPlan_ShouldReturnEmptyResult() {
        // Given: A null query plan
        QueryPlan plan = null;
        
        // When: ExecuteWithPagination is called
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, null, 10);
        
        // Then: Should return empty result
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).isEmpty();
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteWithPagination_WithEmptyPlan_ShouldReturnEmptyResult() {
        // Given: An empty query plan
        QueryPlan plan = new QueryPlan();
        
        // When: ExecuteWithPagination is called
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, null, 10);
        
        // Then: Should return empty result
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).isEmpty();
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteWithPagination_WithFirstPage_ShouldReturnResultsWithoutCursor() throws Exception {
        // Given: A query for the first page (no cursor)
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(11); // pageSize + 1
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock 5 results (less than page size of 10)
        SearchHit[] hits = new SearchHit[5];
        for (int i = 0; i < 5; i++) {
            Map<String, Object> source = new HashMap<>();
            source.put("_id", "id" + i);
            source.put("time", "2022-05-13T10:0" + i + ":00Z");
            source.put("message", "Event " + i);
            hits[i] = createMockSearchHit("id" + i, "aegis-events-2022-05-13", 1.0f, source);
        }
        
        when(searchHits.getHits()).thenReturn(hits);
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(5,
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
        
        // When: ExecuteWithPagination is called with no cursor
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, null, 10);
        
        // Then: Should return 5 results with hasMore=false and no cursor
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(5);
                assertThat(queryResult.getPageSize()).isEqualTo(10);
                assertThat(queryResult.isHasMore()).isFalse();
                assertThat(queryResult.getCursor()).isNull();
                assertThat(queryResult.getTotalCount()).isEqualTo(5);
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteWithPagination_WithFullPage_ShouldReturnResultsWithCursor() throws Exception {
        // Given: A query that returns exactly pageSize + 1 results
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(11); // pageSize + 1
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock 11 results (pageSize + 1, indicating more results exist)
        SearchHit[] hits = new SearchHit[11];
        for (int i = 0; i < 11; i++) {
            Map<String, Object> source = new HashMap<>();
            source.put("_id", "id" + i);
            source.put("time", "2022-05-13T10:" + String.format("%02d", i) + ":00Z");
            source.put("message", "Event " + i);
            hits[i] = createMockSearchHit("id" + i, "aegis-events-2022-05-13", 1.0f, source);
        }
        
        when(searchHits.getHits()).thenReturn(hits);
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(11,
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
        
        // When: ExecuteWithPagination is called with pageSize=10
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, null, 10);
        
        // Then: Should return 10 results (trimmed) with hasMore=true and a cursor
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(10); // Trimmed from 11
                assertThat(queryResult.getPageSize()).isEqualTo(10);
                assertThat(queryResult.isHasMore()).isTrue();
                assertThat(queryResult.getCursor()).isNotNull();
                assertThat(queryResult.getCursor()).isNotEmpty();
                assertThat(queryResult.getTotalCount()).isEqualTo(10);
                
                // Verify the last row is id9 (0-indexed)
                Map<String, Object> lastRow = queryResult.getRows().get(9);
                assertThat(lastRow.get("_id")).isEqualTo("id9");
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteWithPagination_WithCursor_ShouldContinueFromLastPosition() throws Exception {
        // Given: A query with a cursor from a previous page
        String cursor = createCursor("id9", "2022-05-13T10:09:00Z", 10);
        
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(11); // pageSize + 1
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock 5 more results (second page)
        SearchHit[] hits = new SearchHit[5];
        for (int i = 0; i < 5; i++) {
            int id = 10 + i; // Continue from id10
            Map<String, Object> source = new HashMap<>();
            source.put("_id", "id" + id);
            source.put("time", "2022-05-13T10:" + String.format("%02d", id) + ":00Z");
            source.put("message", "Event " + id);
            hits[i] = createMockSearchHit("id" + id, "aegis-events-2022-05-13", 1.0f, source);
        }
        
        when(searchHits.getHits()).thenReturn(hits);
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(5,
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
        
        // When: ExecuteWithPagination is called with cursor
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, cursor, 10);
        
        // Then: Should return next 5 results with hasMore=false
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(5);
                assertThat(queryResult.getPageSize()).isEqualTo(10);
                assertThat(queryResult.isHasMore()).isFalse();
                assertThat(queryResult.getCursor()).isNull();
                
                // Verify first row is id10
                Map<String, Object> firstRow = queryResult.getRows().get(0);
                assertThat(firstRow.get("_id")).isEqualTo("id10");
            })
            .verifyComplete();
    }
    
    // ========== Page Size Tests ==========
    
    @Test
    void testExecuteWithPagination_WithZeroPageSize_ShouldNormalizeToOne() throws Exception {
        // Given: A query with page size of 0
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        SearchHit hit = createMockSearchHit("id1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Event 1"));
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
        
        // When: ExecuteWithPagination is called with pageSize=0
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, null, 0);
        
        // Then: Should normalize to pageSize=1
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getPageSize()).isEqualTo(1);
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteWithPagination_WithNegativePageSize_ShouldNormalizeToOne() throws Exception {
        // Given: A query with negative page size
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        SearchHit hit = createMockSearchHit("id1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Event 1"));
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
        
        // When: ExecuteWithPagination is called with pageSize=-5
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, null, -5);
        
        // Then: Should normalize to pageSize=1
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getPageSize()).isEqualTo(1);
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteWithPagination_WithExcessivePageSize_ShouldCapAtMaximum() throws Exception {
        // Given: A query with page size exceeding maximum (10,000)
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        SearchHit hit = createMockSearchHit("id1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Event 1"));
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
        
        // When: ExecuteWithPagination is called with pageSize=50000
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, null, 50000);
        
        // Then: Should cap at maximum pageSize=10000
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getPageSize()).isEqualTo(10000);
            })
            .verifyComplete();
    }
    
    // ========== Cursor Encoding/Decoding Tests ==========
    
    @Test
    void testExecuteWithPagination_WithInvalidCursor_ShouldStartFromBeginning() throws Exception {
        // Given: A query with an invalid cursor
        String invalidCursor = "invalid-base64-cursor!!!";
        
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        SearchHit[] hits = new SearchHit[3];
        for (int i = 0; i < 3; i++) {
            Map<String, Object> source = new HashMap<>();
            source.put("_id", "id" + i);
            source.put("time", "2022-05-13T10:0" + i + ":00Z");
            source.put("message", "Event " + i);
            hits[i] = createMockSearchHit("id" + i, "aegis-events-2022-05-13", 1.0f, source);
        }
        
        when(searchHits.getHits()).thenReturn(hits);
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(3,
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
        
        // When: ExecuteWithPagination is called with invalid cursor
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, invalidCursor, 10);
        
        // Then: Should start from beginning (offset 0)
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(3);
                // Should start from id0, not continue from cursor
                Map<String, Object> firstRow = queryResult.getRows().get(0);
                assertThat(firstRow.get("_id")).isEqualTo("id0");
            })
            .verifyComplete();
    }
    
    @Test
    void testExecuteWithPagination_WithEmptyCursor_ShouldStartFromBeginning() throws Exception {
        // Given: A query with an empty cursor
        String emptyCursor = "";
        
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery());
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        SearchHit hit = createMockSearchHit("id1", "aegis-events-2022-05-13", 1.0f,
            Map.of("message", "Event 1"));
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
        
        // When: ExecuteWithPagination is called with empty cursor
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, emptyCursor, 10);
        
        // Then: Should start from beginning
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(1);
            })
            .verifyComplete();
    }
    
    // ========== Large Result Set Tests ==========
    
    @Test
    void testExecuteWithPagination_WithLargeResultSet_ShouldHandleMultiplePages() throws Exception {
        // Given: A large result set requiring multiple pages
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(101); // pageSize + 1
        OpenSearchQuery query = new OpenSearchQuery(sourceBuilder);
        
        // Mock 101 results (indicating more results exist)
        SearchHit[] hits = new SearchHit[101];
        for (int i = 0; i < 101; i++) {
            Map<String, Object> source = new HashMap<>();
            source.put("_id", "id" + i);
            source.put("time", "2022-05-13T" + String.format("%02d", i / 60) + ":" + 
                       String.format("%02d", i % 60) + ":00Z");
            source.put("message", "Event " + i);
            hits[i] = createMockSearchHit("id" + i, "aegis-events-2022-05-13", 1.0f, source);
        }
        
        when(searchHits.getHits()).thenReturn(hits);
        when(searchHits.getTotalHits()).thenReturn(new org.apache.lucene.search.TotalHits(101,
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
        
        // When: ExecuteWithPagination is called with pageSize=100
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, null, 100);
        
        // Then: Should return 100 results with hasMore=true and cursor
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(100); // Trimmed from 101
                assertThat(queryResult.getPageSize()).isEqualTo(100);
                assertThat(queryResult.isHasMore()).isTrue();
                assertThat(queryResult.getCursor()).isNotNull();
                assertThat(queryResult.getTotalCount()).isEqualTo(100);
                
                // Verify last row is id99
                Map<String, Object> lastRow = queryResult.getRows().get(99);
                assertThat(lastRow.get("_id")).isEqualTo("id99");
            })
            .verifyComplete();
    }
    
    // ========== ClickHouse Pagination Tests ==========
    
    @Test
    void testExecuteWithPagination_WithClickHouseQuery_ShouldApplyLimitOffset() {
        // Given: A ClickHouse query with pagination
        String sql = "SELECT * FROM aegis_events_warm WHERE severity >= 3";
        ClickHouseQuery query = new ClickHouseQuery(sql);
        
        // Mock JDBC template to return results
        when(clickHouseTemplate.query(anyString(), any(org.springframework.jdbc.core.ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                String executedSql = invocation.getArgument(0);
                
                // Verify LIMIT and OFFSET were added
                assertThat(executedSql).contains("LIMIT");
                assertThat(executedSql).contains("OFFSET");
                
                org.springframework.jdbc.core.ResultSetExtractor<?> extractor = invocation.getArgument(1);
                
                // Create mock ResultSet with 5 results
                java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                java.sql.ResultSetMetaData metaData = mock(java.sql.ResultSetMetaData.class);
                
                when(metaData.getColumnCount()).thenReturn(2);
                when(metaData.getColumnLabel(1)).thenReturn("id");
                when(metaData.getColumnLabel(2)).thenReturn("message");
                when(rs.getMetaData()).thenReturn(metaData);
                
                when(rs.next()).thenReturn(true, true, true, true, true, false);
                when(rs.getObject(1)).thenReturn("id1", "id2", "id3", "id4", "id5");
                when(rs.getObject(2)).thenReturn("Event 1", "Event 2", "Event 3", "Event 4", "Event 5");
                
                return extractor.extractData(rs);
            });
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: ExecuteWithPagination is called
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, null, 10);
        
        // Then: Should return results with LIMIT and OFFSET applied
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
                assertThat(queryResult.getRows()).hasSize(5);
                assertThat(queryResult.getStorageTier()).isEqualTo(StorageTier.WARM);
            })
            .verifyComplete();
        
        // Verify JDBC template was called with modified SQL
        verify(clickHouseTemplate).query(contains("LIMIT"), 
            any(org.springframework.jdbc.core.ResultSetExtractor.class));
    }
    
    @Test
    void testExecuteWithPagination_WithClickHouseAndCursor_ShouldApplyOffset() {
        // Given: A ClickHouse query with cursor (offset=10)
        String cursor = createCursor("id9", "2022-05-13T10:09:00Z", 10);
        String sql = "SELECT * FROM aegis_events_warm WHERE severity >= 3";
        ClickHouseQuery query = new ClickHouseQuery(sql);
        
        // Mock JDBC template
        when(clickHouseTemplate.query(anyString(), any(org.springframework.jdbc.core.ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                String executedSql = invocation.getArgument(0);
                
                // Verify OFFSET 10 was applied
                assertThat(executedSql).contains("OFFSET 10");
                
                org.springframework.jdbc.core.ResultSetExtractor<?> extractor = invocation.getArgument(1);
                
                java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                java.sql.ResultSetMetaData metaData = mock(java.sql.ResultSetMetaData.class);
                
                when(metaData.getColumnCount()).thenReturn(1);
                when(metaData.getColumnLabel(1)).thenReturn("id");
                when(rs.getMetaData()).thenReturn(metaData);
                
                when(rs.next()).thenReturn(true, false);
                when(rs.getObject(1)).thenReturn("id10");
                
                return extractor.extractData(rs);
            });
        
        QueryPlan plan = new QueryPlan();
        plan.addSubQuery(query);
        
        // When: ExecuteWithPagination is called with cursor
        Flux<QueryResult> result = queryExecutor.executeWithPagination(plan, cursor, 10);
        
        // Then: Should apply offset from cursor
        StepVerifier.create(result)
            .assertNext(queryResult -> {
                assertThat(queryResult).isNotNull();
            })
            .verifyComplete();
        
        verify(clickHouseTemplate).query(contains("OFFSET 10"),
            any(org.springframework.jdbc.core.ResultSetExtractor.class));
    }
}
