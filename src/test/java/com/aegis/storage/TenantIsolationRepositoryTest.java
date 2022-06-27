package com.aegis.storage;

import com.aegis.domain.Alert;
import com.aegis.domain.AlertStatus;
import com.aegis.domain.OcsfEvent;
import com.aegis.domain.QueryResult;
import com.aegis.security.TenantContext;
import com.aegis.storage.hot.AlertRepository;
import com.aegis.storage.hot.OpenSearchRepository;
import com.aegis.storage.warm.ClickHouseRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for tenant isolation in repository classes
 * 
 * These tests verify that:
 * 1. All repository methods inject tenant_id filters
 * 2. Queries fail when no tenant context is set
 * 3. Different tenants cannot access each other's data
 */
@ExtendWith(MockitoExtension.class)
class TenantIsolationRepositoryTest {
    
    @Mock
    private RestHighLevelClient openSearchClient;
    
    @Mock
    private JdbcTemplate clickHouseTemplate;
    
    @Mock
    private ObjectMapper objectMapper;
    
    @Mock
    private SearchResponse searchResponse;
    
    @Mock
    private SearchHits searchHits;
    
    private AlertRepository alertRepository;
    private OpenSearchRepository openSearchRepository;
    private ClickHouseRepository clickHouseRepository;
    
    @BeforeEach
    void setUp() {
        alertRepository = new AlertRepository();
        alertRepository.client = openSearchClient;
        alertRepository.objectMapper = objectMapper;
        
        openSearchRepository = new OpenSearchRepository();
        openSearchRepository.client = openSearchClient;
        openSearchRepository.objectMapper = objectMapper;
        
        clickHouseRepository = new ClickHouseRepository();
        clickHouseRepository.jdbcTemplate = clickHouseTemplate;
        
        // Clear tenant context before each test
        TenantContext.clear();
    }
    
    @AfterEach
    void tearDown() {
        // Always clear tenant context after each test
        TenantContext.clear();
    }
    
    @Test
    void testAlertRepository_FindAlerts_InjectsTenantFilter() throws Exception {
        // Given: A tenant context is set
        String tenantId = "tenant-123";
        TenantContext.setTenantId(tenantId);
        
        // And: Mock OpenSearch response
        when(searchHits.getHits()).thenReturn(new org.opensearch.search.SearchHit[0]);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(openSearchClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(searchResponse);
        
        // When: Finding alerts
        alertRepository.findAlerts(null, null, 0L, System.currentTimeMillis(), 10, 0);
        
        // Then: The search request should contain tenant_id filter
        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));
        
        SearchRequest capturedRequest = requestCaptor.getValue();
        SearchSourceBuilder sourceBuilder = capturedRequest.source();
        
        assertThat(sourceBuilder.query()).isInstanceOf(BoolQueryBuilder.class);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) sourceBuilder.query();
        
        boolean hasTenantFilter = boolQuery.filter().stream()
            .anyMatch(query -> {
                if (query instanceof TermQueryBuilder) {
                    TermQueryBuilder termQuery = (TermQueryBuilder) query;
                    return termQuery.fieldName().equals("tenant_id") && 
                           termQuery.value().equals(tenantId);
                }
                return false;
            });
        
        assertThat(hasTenantFilter)
            .as("Alert query should contain tenant_id filter")
            .isTrue();
    }
    
    @Test
    void testAlertRepository_FindAlerts_FailsWithoutTenantContext() {
        // Given: No tenant context is set
        TenantContext.clear();
        
        // When/Then: Finding alerts should throw IllegalStateException
        assertThatThrownBy(() -> 
            alertRepository.findAlerts(null, null, 0L, System.currentTimeMillis(), 10, 0))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No tenant ID set in current context");
    }
    
    @Test
    void testAlertRepository_FindById_InjectsTenantFilter() throws Exception {
        // Given: A tenant context is set
        String tenantId = "tenant-456";
        TenantContext.setTenantId(tenantId);
        
        // And: Mock OpenSearch response
        when(searchHits.getHits()).thenReturn(new org.opensearch.search.SearchHit[0]);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(openSearchClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(searchResponse);
        
        // When: Finding alert by ID
        alertRepository.findById("alert-123");
        
        // Then: The search request should contain tenant_id filter
        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));
        
        SearchRequest capturedRequest = requestCaptor.getValue();
        SearchSourceBuilder sourceBuilder = capturedRequest.source();
        
        assertThat(sourceBuilder.query()).isInstanceOf(BoolQueryBuilder.class);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) sourceBuilder.query();
        
        boolean hasTenantFilter = boolQuery.filter().stream()
            .anyMatch(query -> {
                if (query instanceof TermQueryBuilder) {
                    TermQueryBuilder termQuery = (TermQueryBuilder) query;
                    return termQuery.fieldName().equals("tenant_id") && 
                           termQuery.value().equals(tenantId);
                }
                return false;
            });
        
        assertThat(hasTenantFilter)
            .as("Alert findById query should contain tenant_id filter")
            .isTrue();
    }
    
    @Test
    void testAlertRepository_CountAlerts_InjectsTenantFilter() throws Exception {
        // Given: A tenant context is set
        String tenantId = "tenant-789";
        TenantContext.setTenantId(tenantId);
        
        // And: Mock OpenSearch response
        when(searchHits.getHits()).thenReturn(new org.opensearch.search.SearchHit[0]);
        when(searchHits.getTotalHits()).thenReturn(
            new org.apache.lucene.search.TotalHits(0, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO));
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(openSearchClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(searchResponse);
        
        // When: Counting alerts
        alertRepository.countAlerts(null, null, 0L, System.currentTimeMillis());
        
        // Then: The search request should contain tenant_id filter
        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));
        
        SearchRequest capturedRequest = requestCaptor.getValue();
        SearchSourceBuilder sourceBuilder = capturedRequest.source();
        
        assertThat(sourceBuilder.query()).isInstanceOf(BoolQueryBuilder.class);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) sourceBuilder.query();
        
        boolean hasTenantFilter = boolQuery.filter().stream()
            .anyMatch(query -> {
                if (query instanceof TermQueryBuilder) {
                    TermQueryBuilder termQuery = (TermQueryBuilder) query;
                    return termQuery.fieldName().equals("tenant_id") && 
                           termQuery.value().equals(tenantId);
                }
                return false;
            });
        
        assertThat(hasTenantFilter)
            .as("Alert count query should contain tenant_id filter")
            .isTrue();
    }
    
    @Test
    void testOpenSearchRepository_Search_InjectsTenantFilter() throws Exception {
        // Given: A tenant context is set
        String tenantId = "tenant-abc";
        TenantContext.setTenantId(tenantId);
        
        // And: Mock OpenSearch response
        when(searchHits.getHits()).thenReturn(new org.opensearch.search.SearchHit[0]);
        when(searchHits.getTotalHits()).thenReturn(
            new org.apache.lucene.search.TotalHits(0, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO));
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchResponse.getTook()).thenReturn(new org.opensearch.core.common.unit.TimeValue(10));
        when(openSearchClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(searchResponse);
        
        // When: Searching events
        QueryBuilder query = org.opensearch.index.query.QueryBuilders.matchAllQuery();
        openSearchRepository.search(query, 0, 10);
        
        // Then: The search request should contain tenant_id filter
        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));
        
        SearchRequest capturedRequest = requestCaptor.getValue();
        SearchSourceBuilder sourceBuilder = capturedRequest.source();
        
        assertThat(sourceBuilder.query()).isInstanceOf(BoolQueryBuilder.class);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) sourceBuilder.query();
        
        boolean hasTenantFilter = boolQuery.filter().stream()
            .anyMatch(query1 -> {
                if (query1 instanceof TermQueryBuilder) {
                    TermQueryBuilder termQuery = (TermQueryBuilder) query1;
                    return termQuery.fieldName().equals("tenant_id") && 
                           termQuery.value().equals(tenantId);
                }
                return false;
            });
        
        assertThat(hasTenantFilter)
            .as("OpenSearch search query should contain tenant_id filter")
            .isTrue();
    }
    
    @Test
    void testOpenSearchRepository_AggregateByField_InjectsTenantFilter() throws Exception {
        // Given: A tenant context is set
        String tenantId = "tenant-def";
        TenantContext.setTenantId(tenantId);
        
        // And: Mock OpenSearch response with aggregations
        org.opensearch.search.aggregations.Aggregations aggregations = mock(org.opensearch.search.aggregations.Aggregations.class);
        org.opensearch.search.aggregations.bucket.terms.Terms termsAgg = mock(org.opensearch.search.aggregations.bucket.terms.Terms.class);
        when(termsAgg.getBuckets()).thenReturn(List.of());
        when(aggregations.get("by_field")).thenReturn(termsAgg);
        when(searchResponse.getAggregations()).thenReturn(aggregations);
        when(openSearchClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(searchResponse);
        
        // When: Aggregating by field
        openSearchRepository.aggregateByField("category_name", 10);
        
        // Then: The search request should contain tenant_id filter
        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));
        
        SearchRequest capturedRequest = requestCaptor.getValue();
        SearchSourceBuilder sourceBuilder = capturedRequest.source();
        
        assertThat(sourceBuilder.query()).isInstanceOf(TermQueryBuilder.class);
        TermQueryBuilder termQuery = (TermQueryBuilder) sourceBuilder.query();
        
        assertThat(termQuery.fieldName())
            .as("Aggregation query should filter by tenant_id")
            .isEqualTo("tenant_id");
        assertThat(termQuery.value())
            .as("Aggregation query should use correct tenant ID")
            .isEqualTo(tenantId);
    }
    
    @Test
    void testClickHouseRepository_SearchByTimeRange_InjectsTenantFilter() {
        // Given: A tenant context is set
        String tenantId = "tenant-ghi";
        TenantContext.setTenantId(tenantId);
        
        // And: Mock ClickHouse response
        when(clickHouseTemplate.query(anyString(), any(), any(), any(), any(), any()))
            .thenReturn(List.of());
        
        // When: Searching by time range
        clickHouseRepository.searchByTimeRange(0L, System.currentTimeMillis(), 10);
        
        // Then: The SQL should contain tenant_id filter
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(clickHouseTemplate).query(sqlCaptor.capture(), any(), any(), any(), any(), any());
        
        String capturedSql = sqlCaptor.getValue();
        assertThat(capturedSql)
            .as("ClickHouse query should contain tenant_id filter")
            .contains("WHERE tenant_id = ?");
    }
    
    @Test
    void testClickHouseRepository_SearchByTimeRange_FailsWithoutTenantContext() {
        // Given: No tenant context is set
        TenantContext.clear();
        
        // When/Then: Searching should throw IllegalStateException
        assertThatThrownBy(() -> 
            clickHouseRepository.searchByTimeRange(0L, System.currentTimeMillis(), 10))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No tenant ID set in current context");
    }
    
    @Test
    void testClickHouseRepository_AggregateByCategory_InjectsTenantFilter() {
        // Given: A tenant context is set
        String tenantId = "tenant-jkl";
        TenantContext.setTenantId(tenantId);
        
        // And: Mock ClickHouse response
        when(clickHouseTemplate.queryForList(anyString(), any(), any(), any()))
            .thenReturn(List.of());
        
        // When: Aggregating by category
        clickHouseRepository.aggregateByCategory(0L, System.currentTimeMillis());
        
        // Then: The SQL should contain tenant_id filter
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(clickHouseTemplate).queryForList(sqlCaptor.capture(), any(), any(), any());
        
        String capturedSql = sqlCaptor.getValue();
        assertThat(capturedSql)
            .as("ClickHouse aggregation should contain tenant_id filter")
            .contains("WHERE tenant_id = ?");
    }
    
    @Test
    void testClickHouseRepository_GetTopUsers_InjectsTenantFilter() {
        // Given: A tenant context is set
        String tenantId = "tenant-mno";
        TenantContext.setTenantId(tenantId);
        
        // And: Mock ClickHouse response
        when(clickHouseTemplate.queryForList(anyString(), any(), any(), any(), any()))
            .thenReturn(List.of());
        
        // When: Getting top users
        clickHouseRepository.getTopUsers(0L, System.currentTimeMillis(), 10);
        
        // Then: The SQL should contain tenant_id filter
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(clickHouseTemplate).queryForList(sqlCaptor.capture(), any(), any(), any(), any());
        
        String capturedSql = sqlCaptor.getValue();
        assertThat(capturedSql)
            .as("ClickHouse top users query should contain tenant_id filter")
            .contains("WHERE tenant_id = ?");
    }
    
    @Test
    void testTenantIsolation_DifferentTenantsCannotAccessEachOthersData() throws Exception {
        // Given: First tenant context
        String tenant1 = "tenant-001";
        TenantContext.setTenantId(tenant1);
        
        // And: Mock OpenSearch response
        when(searchHits.getHits()).thenReturn(new org.opensearch.search.SearchHit[0]);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(openSearchClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(searchResponse);
        
        // When: Finding alerts for tenant 1
        alertRepository.findAlerts(null, null, 0L, System.currentTimeMillis(), 10, 0);
        
        ArgumentCaptor<SearchRequest> request1Captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient).search(request1Captor.capture(), eq(RequestOptions.DEFAULT));
        
        // Then: Switch to second tenant
        TenantContext.clear();
        String tenant2 = "tenant-002";
        TenantContext.setTenantId(tenant2);
        
        // When: Finding alerts for tenant 2
        alertRepository.findAlerts(null, null, 0L, System.currentTimeMillis(), 10, 0);
        
        ArgumentCaptor<SearchRequest> request2Captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient, times(2)).search(request2Captor.capture(), eq(RequestOptions.DEFAULT));
        
        // Then: Verify different tenant filters
        List<SearchRequest> allRequests = request2Captor.getAllValues();
        SearchRequest req1 = allRequests.get(0);
        SearchRequest req2 = allRequests.get(1);
        
        BoolQueryBuilder query1 = (BoolQueryBuilder) req1.source().query();
        BoolQueryBuilder query2 = (BoolQueryBuilder) req2.source().query();
        
        // Extract tenant IDs from queries
        String extractedTenant1 = extractTenantIdFromQuery(query1);
        String extractedTenant2 = extractTenantIdFromQuery(query2);
        
        assertThat(extractedTenant1)
            .as("First query should filter by tenant-001")
            .isEqualTo(tenant1);
        
        assertThat(extractedTenant2)
            .as("Second query should filter by tenant-002")
            .isEqualTo(tenant2);
        
        assertThat(extractedTenant1)
            .as("Tenants should be different")
            .isNotEqualTo(extractedTenant2);
    }
    
    // Helper method to extract tenant ID from bool query
    private String extractTenantIdFromQuery(BoolQueryBuilder boolQuery) {
        return boolQuery.filter().stream()
            .filter(query -> query instanceof TermQueryBuilder)
            .map(query -> (TermQueryBuilder) query)
            .filter(termQuery -> termQuery.fieldName().equals("tenant_id"))
            .map(termQuery -> (String) termQuery.value())
            .findFirst()
            .orElse(null);
    }
}
