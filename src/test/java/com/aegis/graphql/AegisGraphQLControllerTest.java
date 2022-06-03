package com.aegis.graphql;

import com.aegis.domain.QueryResult;
import com.aegis.query.AqlTranspiler;
import com.aegis.query.QueryExecutor;
import com.aegis.query.QueryPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AegisGraphQLController
 */
@ExtendWith(MockitoExtension.class)
class AegisGraphQLControllerTest {
    
    @Mock
    private AqlTranspiler aqlTranspiler;
    
    @Mock
    private QueryExecutor queryExecutor;
    
    private AegisGraphQLController controller;
    
    @BeforeEach
    void setUp() {
        controller = new AegisGraphQLController(aqlTranspiler, queryExecutor);
    }
    
    @Test
    void testSearchEvents_Success() {
        // Arrange
        String query = "where severity >= 4";
        String timeRange = "last 24h";
        Integer limit = 100;
        
        QueryPlan mockPlan = new QueryPlan();
        when(aqlTranspiler.transpile(anyString())).thenReturn(mockPlan);
        
        QueryResult mockResult = new QueryResult();
        List<Map<String, Object>> rows = new ArrayList<>();
        
        Map<String, Object> event1 = new HashMap<>();
        event1.put("id", "event-1");
        event1.put("severity", 4);
        event1.put("message", "High severity event");
        rows.add(event1);
        
        Map<String, Object> event2 = new HashMap<>();
        event2.put("id", "event-2");
        event2.put("severity", 5);
        event2.put("message", "Critical event");
        rows.add(event2);
        
        mockResult.setRows(rows);
        mockResult.setTotalCount(2);
        
        when(queryExecutor.execute(any(QueryPlan.class)))
            .thenReturn(Flux.just(mockResult));
        
        // Act
        Flux<Map<String, Object>> result = controller.searchEvents(query, timeRange, limit);
        
        // Assert
        StepVerifier.create(result)
            .expectNextMatches(event -> "event-1".equals(event.get("id")))
            .expectNextMatches(event -> "event-2".equals(event.get("id")))
            .verifyComplete();
        
        // Verify interactions
        verify(aqlTranspiler, times(1)).transpile(anyString());
        verify(queryExecutor, times(1)).execute(any(QueryPlan.class));
    }
    
    @Test
    void testSearchEvents_WithNullQuery() {
        // Arrange
        String query = null;
        String timeRange = "last 24h";
        Integer limit = 100;
        
        QueryPlan mockPlan = new QueryPlan();
        when(aqlTranspiler.transpile(anyString())).thenReturn(mockPlan);
        
        QueryResult mockResult = new QueryResult();
        mockResult.setRows(new ArrayList<>());
        mockResult.setTotalCount(0);
        
        when(queryExecutor.execute(any(QueryPlan.class)))
            .thenReturn(Flux.just(mockResult));
        
        // Act
        Flux<Map<String, Object>> result = controller.searchEvents(query, timeRange, limit);
        
        // Assert
        StepVerifier.create(result)
            .verifyComplete();
        
        // Verify the AQL query was constructed correctly (source=* last 24h | head 100)
        verify(aqlTranspiler, times(1)).transpile(contains("source=*"));
        verify(aqlTranspiler, times(1)).transpile(contains("last 24h"));
        verify(aqlTranspiler, times(1)).transpile(contains("head 100"));
    }
    
    @Test
    void testSearchEvents_WithEmptyTimeRange() {
        // Arrange
        String query = "where severity >= 4";
        String timeRange = "";
        Integer limit = 50;
        
        QueryPlan mockPlan = new QueryPlan();
        when(aqlTranspiler.transpile(anyString())).thenReturn(mockPlan);
        
        QueryResult mockResult = new QueryResult();
        mockResult.setRows(new ArrayList<>());
        
        when(queryExecutor.execute(any(QueryPlan.class)))
            .thenReturn(Flux.just(mockResult));
        
        // Act
        Flux<Map<String, Object>> result = controller.searchEvents(query, timeRange, limit);
        
        // Assert
        StepVerifier.create(result)
            .verifyComplete();
        
        // Verify the AQL query was constructed without time range
        verify(aqlTranspiler, times(1)).transpile(contains("source=*"));
        verify(aqlTranspiler, times(1)).transpile(contains("where severity >= 4"));
    }
    
    @Test
    void testSearchEvents_WithQueryContainingPipe() {
        // Arrange
        String query = "| where severity >= 4 | stats count() by actor.user";
        String timeRange = "last 7d";
        Integer limit = 200;
        
        QueryPlan mockPlan = new QueryPlan();
        when(aqlTranspiler.transpile(anyString())).thenReturn(mockPlan);
        
        QueryResult mockResult = new QueryResult();
        mockResult.setRows(new ArrayList<>());
        
        when(queryExecutor.execute(any(QueryPlan.class)))
            .thenReturn(Flux.just(mockResult));
        
        // Act
        Flux<Map<String, Object>> result = controller.searchEvents(query, timeRange, limit);
        
        // Assert
        StepVerifier.create(result)
            .verifyComplete();
        
        // Verify the AQL query doesn't have double pipes
        verify(aqlTranspiler, times(1)).transpile(argThat(aql -> 
            !aql.contains("||") && aql.contains("where severity >= 4")));
    }
    
    @Test
    void testSearchEvents_WithQueryContainingHeadClause() {
        // Arrange
        String query = "where severity >= 4 | head 50";
        String timeRange = "last 24h";
        Integer limit = 100;
        
        QueryPlan mockPlan = new QueryPlan();
        when(aqlTranspiler.transpile(anyString())).thenReturn(mockPlan);
        
        QueryResult mockResult = new QueryResult();
        mockResult.setRows(new ArrayList<>());
        
        when(queryExecutor.execute(any(QueryPlan.class)))
            .thenReturn(Flux.just(mockResult));
        
        // Act
        Flux<Map<String, Object>> result = controller.searchEvents(query, timeRange, limit);
        
        // Assert
        StepVerifier.create(result)
            .verifyComplete();
        
        // Verify the AQL query doesn't add another head clause
        verify(aqlTranspiler, times(1)).transpile(argThat(aql -> {
            int headCount = aql.split("head", -1).length - 1;
            return headCount == 1; // Should only have one "head" clause
        }));
    }
    
    @Test
    void testSearchEvents_TranspilerError() {
        // Arrange
        String query = "invalid query syntax";
        String timeRange = "last 24h";
        Integer limit = 100;
        
        when(aqlTranspiler.transpile(anyString()))
            .thenThrow(new RuntimeException("Invalid AQL syntax"));
        
        // Act
        Flux<Map<String, Object>> result = controller.searchEvents(query, timeRange, limit);
        
        // Assert - should return empty flux on error
        StepVerifier.create(result)
            .verifyComplete();
        
        verify(aqlTranspiler, times(1)).transpile(anyString());
        verify(queryExecutor, never()).execute(any(QueryPlan.class));
    }
    
    @Test
    void testSearchEvents_ExecutorError() {
        // Arrange
        String query = "where severity >= 4";
        String timeRange = "last 24h";
        Integer limit = 100;
        
        QueryPlan mockPlan = new QueryPlan();
        when(aqlTranspiler.transpile(anyString())).thenReturn(mockPlan);
        
        when(queryExecutor.execute(any(QueryPlan.class)))
            .thenReturn(Flux.error(new RuntimeException("Query execution failed")));
        
        // Act
        Flux<Map<String, Object>> result = controller.searchEvents(query, timeRange, limit);
        
        // Assert - error should be propagated
        StepVerifier.create(result)
            .expectError(RuntimeException.class)
            .verify();
        
        verify(aqlTranspiler, times(1)).transpile(anyString());
        verify(queryExecutor, times(1)).execute(any(QueryPlan.class));
    }
    
    @Test
    void testSearchEvents_EmptyResult() {
        // Arrange
        String query = "where severity >= 4";
        String timeRange = "last 24h";
        Integer limit = 100;
        
        QueryPlan mockPlan = new QueryPlan();
        when(aqlTranspiler.transpile(anyString())).thenReturn(mockPlan);
        
        QueryResult mockResult = new QueryResult();
        mockResult.setRows(new ArrayList<>());
        mockResult.setTotalCount(0);
        
        when(queryExecutor.execute(any(QueryPlan.class)))
            .thenReturn(Flux.just(mockResult));
        
        // Act
        Flux<Map<String, Object>> result = controller.searchEvents(query, timeRange, limit);
        
        // Assert
        StepVerifier.create(result)
            .verifyComplete();
        
        verify(aqlTranspiler, times(1)).transpile(anyString());
        verify(queryExecutor, times(1)).execute(any(QueryPlan.class));
    }
    
    @Test
    void testSearchEvents_LargeResultSet() {
        // Arrange
        String query = "where severity >= 1";
        String timeRange = "last 7d";
        Integer limit = 1000;
        
        QueryPlan mockPlan = new QueryPlan();
        when(aqlTranspiler.transpile(anyString())).thenReturn(mockPlan);
        
        QueryResult mockResult = new QueryResult();
        List<Map<String, Object>> rows = new ArrayList<>();
        
        // Create 1000 mock events
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> event = new HashMap<>();
            event.put("id", "event-" + i);
            event.put("severity", (i % 5) + 1);
            event.put("message", "Event " + i);
            rows.add(event);
        }
        
        mockResult.setRows(rows);
        mockResult.setTotalCount(1000);
        
        when(queryExecutor.execute(any(QueryPlan.class)))
            .thenReturn(Flux.just(mockResult));
        
        // Act
        Flux<Map<String, Object>> result = controller.searchEvents(query, timeRange, limit);
        
        // Assert
        StepVerifier.create(result)
            .expectNextCount(1000)
            .verifyComplete();
        
        verify(aqlTranspiler, times(1)).transpile(anyString());
        verify(queryExecutor, times(1)).execute(any(QueryPlan.class));
    }
}
