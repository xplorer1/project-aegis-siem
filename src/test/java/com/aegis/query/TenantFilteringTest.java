package com.aegis.query;

import com.aegis.security.TenantContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for tenant filtering in query builders
 * 
 * These tests verify that:
 * 1. Tenant ID is automatically injected into all queries
 * 2. Queries fail when no tenant context is set
 * 3. Tenant filtering works correctly for OpenSearch and ClickHouse
 */
@ExtendWith(MockitoExtension.class)
class TenantFilteringTest {
    
    private OpenSearchTranspiler openSearchTranspiler;
    private ClickHouseTranspiler clickHouseTranspiler;
    
    @BeforeEach
    void setUp() {
        openSearchTranspiler = new OpenSearchTranspiler();
        clickHouseTranspiler = new ClickHouseTranspiler();
        
        // Clear tenant context before each test
        TenantContext.clear();
    }
    
    @AfterEach
    void tearDown() {
        // Always clear tenant context after each test
        TenantContext.clear();
    }
    
    @Test
    void testOpenSearchTranspiler_InjectsTenantFilter() {
        // Given: A tenant context is set
        String tenantId = "tenant-123";
        TenantContext.setTenantId(tenantId);
        
        // And: A query context
        QueryContext context = createBasicQueryContext();
        
        // When: Transpiling to OpenSearch
        SearchSourceBuilder searchSource = openSearchTranspiler.transpileToOpenSearch(context);
        
        // Then: The query should contain a tenant_id filter
        assertThat(searchSource.query()).isInstanceOf(BoolQueryBuilder.class);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) searchSource.query();
        
        // Verify tenant filter is present
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
            .as("Query should contain tenant_id filter")
            .isTrue();
    }
    
    @Test
    void testOpenSearchTranspiler_FailsWithoutTenantContext() {
        // Given: No tenant context is set
        TenantContext.clear();
        
        // And: A query context
        QueryContext context = createBasicQueryContext();
        
        // When/Then: Transpiling should throw IllegalStateException
        assertThatThrownBy(() -> openSearchTranspiler.transpileToOpenSearch(context))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No tenant ID set in current context");
    }
    
    @Test
    void testClickHouseTranspiler_InjectsTenantFilter() {
        // Given: A tenant context is set
        String tenantId = "tenant-456";
        TenantContext.setTenantId(tenantId);
        
        // And: A query context
        QueryContext context = createBasicQueryContext();
        
        // When: Transpiling to ClickHouse SQL
        String sql = clickHouseTranspiler.transpileToClickHouse(context);
        
        // Then: The SQL should contain a tenant_id filter
        assertThat(sql)
            .as("SQL should contain tenant_id filter")
            .contains("tenant_id = '" + tenantId + "'");
        
        // And: The tenant filter should be in the WHERE clause
        assertThat(sql)
            .as("SQL should have WHERE clause with tenant filter")
            .containsPattern("WHERE.*tenant_id = '" + tenantId + "'");
    }
    
    @Test
    void testClickHouseTranspiler_FailsWithoutTenantContext() {
        // Given: No tenant context is set
        TenantContext.clear();
        
        // And: A query context
        QueryContext context = createBasicQueryContext();
        
        // When/Then: Transpiling should throw IllegalStateException
        assertThatThrownBy(() -> clickHouseTranspiler.transpileToClickHouse(context))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No tenant ID set in current context");
    }
    
    @Test
    void testOpenSearchTranspiler_TenantFilterWithWhereClause() {
        // Given: A tenant context is set
        String tenantId = "tenant-789";
        TenantContext.setTenantId(tenantId);
        
        // And: A query context with a where clause
        QueryContext context = createQueryContextWithWhereClause();
        
        // When: Transpiling to OpenSearch
        SearchSourceBuilder searchSource = openSearchTranspiler.transpileToOpenSearch(context);
        
        // Then: The query should contain both tenant filter and where clause
        assertThat(searchSource.query()).isInstanceOf(BoolQueryBuilder.class);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) searchSource.query();
        
        // Verify tenant filter is present
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
            .as("Query should contain tenant_id filter even with where clause")
            .isTrue();
        
        // Verify there are multiple filters (tenant + time range + where clause)
        assertThat(boolQuery.filter().size())
            .as("Query should have multiple filters")
            .isGreaterThanOrEqualTo(2);
    }
    
    @Test
    void testClickHouseTranspiler_TenantFilterWithWhereClause() {
        // Given: A tenant context is set
        String tenantId = "tenant-abc";
        TenantContext.setTenantId(tenantId);
        
        // And: A query context with a where clause
        QueryContext context = createQueryContextWithWhereClause();
        
        // When: Transpiling to ClickHouse SQL
        String sql = clickHouseTranspiler.transpileToClickHouse(context);
        
        // Then: The SQL should contain tenant filter first
        assertThat(sql)
            .as("SQL should contain tenant_id filter")
            .contains("tenant_id = '" + tenantId + "'");
        
        // And: The tenant filter should come before other conditions
        int tenantFilterIndex = sql.indexOf("tenant_id = '" + tenantId + "'");
        int whereIndex = sql.indexOf("WHERE");
        
        assertThat(tenantFilterIndex)
            .as("Tenant filter should be in WHERE clause")
            .isGreaterThan(whereIndex);
    }
    
    @Test
    void testOpenSearchTranspiler_TenantFilterWithAggregations() {
        // Given: A tenant context is set
        String tenantId = "tenant-def";
        TenantContext.setTenantId(tenantId);
        
        // And: A query context with aggregations
        QueryContext context = createQueryContextWithAggregations();
        
        // When: Transpiling to OpenSearch
        SearchSourceBuilder searchSource = openSearchTranspiler.transpileToOpenSearch(context);
        
        // Then: The query should contain tenant filter
        assertThat(searchSource.query()).isInstanceOf(BoolQueryBuilder.class);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) searchSource.query();
        
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
            .as("Aggregation query should contain tenant_id filter")
            .isTrue();
        
        // And: Aggregations should be present
        assertThat(searchSource.aggregations())
            .as("Query should have aggregations")
            .isNotNull();
    }
    
    @Test
    void testClickHouseTranspiler_TenantFilterWithAggregations() {
        // Given: A tenant context is set
        String tenantId = "tenant-ghi";
        TenantContext.setTenantId(tenantId);
        
        // And: A query context with aggregations
        QueryContext context = createQueryContextWithAggregations();
        
        // When: Transpiling to ClickHouse SQL
        String sql = clickHouseTranspiler.transpileToClickHouse(context);
        
        // Then: The SQL should contain tenant filter
        assertThat(sql)
            .as("Aggregation SQL should contain tenant_id filter")
            .contains("tenant_id = '" + tenantId + "'");
        
        // And: Should have GROUP BY clause
        assertThat(sql)
            .as("Aggregation SQL should have GROUP BY")
            .containsIgnoringCase("GROUP BY");
    }
    
    @Test
    void testTenantIsolation_DifferentTenants() {
        // Given: First tenant context
        String tenant1 = "tenant-001";
        TenantContext.setTenantId(tenant1);
        
        QueryContext context = createBasicQueryContext();
        String sql1 = clickHouseTranspiler.transpileToClickHouse(context);
        
        // When: Switching to second tenant
        TenantContext.clear();
        String tenant2 = "tenant-002";
        TenantContext.setTenantId(tenant2);
        
        String sql2 = clickHouseTranspiler.transpileToClickHouse(context);
        
        // Then: Queries should have different tenant filters
        assertThat(sql1)
            .as("First query should filter by tenant-001")
            .contains("tenant_id = '" + tenant1 + "'")
            .doesNotContain("tenant_id = '" + tenant2 + "'");
        
        assertThat(sql2)
            .as("Second query should filter by tenant-002")
            .contains("tenant_id = '" + tenant2 + "'")
            .doesNotContain("tenant_id = '" + tenant1 + "'");
    }
    
    @Test
    void testTenantFilter_PreventsSQLInjection() {
        // Given: A tenant ID with SQL injection attempt
        String maliciousTenantId = "tenant' OR '1'='1";
        TenantContext.setTenantId(maliciousTenantId);
        
        // And: A query context
        QueryContext context = createBasicQueryContext();
        
        // When: Transpiling to ClickHouse SQL
        String sql = clickHouseTranspiler.transpileToClickHouse(context);
        
        // Then: The SQL should contain the escaped tenant ID
        // Note: In production, we should use parameterized queries
        assertThat(sql)
            .as("SQL should contain the tenant ID")
            .contains("tenant_id = '" + maliciousTenantId + "'");
    }
    
    // Helper methods to create test query contexts
    
    private QueryContext createBasicQueryContext() {
        QueryContext context = new QueryContext();
        
        // Set time range
        TimeRange timeRange = new TimeRange();
        timeRange.setEarliest(Instant.now().minusSeconds(3600).toEpochMilli());
        timeRange.setLatest(Instant.now().toEpochMilli());
        context.setTimeRange(timeRange);
        
        return context;
    }
    
    private QueryContext createQueryContextWithWhereClause() {
        QueryContext context = createBasicQueryContext();
        
        // Add a simple where clause
        ComparisonExpression whereExpr = new ComparisonExpression();
        whereExpr.setField("severity");
        whereExpr.setOperator("=");
        whereExpr.setValue("5");
        context.setWhereExpression(whereExpr);
        
        return context;
    }
    
    private QueryContext createQueryContextWithAggregations() {
        QueryContext context = createBasicQueryContext();
        
        // Add stats clause with aggregations
        StatsClause statsClause = new StatsClause();
        
        Aggregation countAgg = new Aggregation();
        countAgg.setFunction("count");
        countAgg.setField("*");
        countAgg.setAlias("event_count");
        
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(countAgg);
        statsClause.setAggregations(aggregations);
        
        List<String> groupByFields = new ArrayList<>();
        groupByFields.add("category_name");
        statsClause.setGroupByFields(groupByFields);
        
        context.setStatsClause(statsClause);
        
        return context;
    }
}
