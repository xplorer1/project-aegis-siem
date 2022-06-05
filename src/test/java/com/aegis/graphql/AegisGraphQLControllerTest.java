package com.aegis.graphql;

import com.aegis.domain.QueryResult;
import com.aegis.query.AqlTranspiler;
import com.aegis.query.QueryExecutor;
import com.aegis.query.QueryPlan;
import com.aegis.storage.hot.AlertRepository;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
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
    
    @Mock
    private AlertRepository alertRepository;
    
    private AegisGraphQLController controller;
    
    @BeforeEach
    void setUp() {
        controller = new AegisGraphQLController(aqlTranspiler, queryExecutor, alertRepository);
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
    
    // ========== Tests for getAlerts resolver ==========
    
    @Test
    void testGetAlerts_Success() {
        // Arrange
        Integer severity = 4;
        String status = "OPEN";
        String timeRange = "last 24h";
        Integer limit = 100;
        Integer offset = 0;
        
        // Create mock alerts
        List<com.aegis.domain.Alert> mockAlerts = new ArrayList<>();
        
        com.aegis.domain.Alert alert1 = new com.aegis.domain.Alert();
        alert1.setId("alert-1");
        alert1.setSeverity(4);
        alert1.setTitle("Brute Force Attack Detected");
        alert1.setStatus(com.aegis.domain.AlertStatus.OPEN);
        mockAlerts.add(alert1);
        
        com.aegis.domain.Alert alert2 = new com.aegis.domain.Alert();
        alert2.setId("alert-2");
        alert2.setSeverity(5);
        alert2.setTitle("Data Exfiltration Detected");
        alert2.setStatus(com.aegis.domain.AlertStatus.OPEN);
        mockAlerts.add(alert2);
        
        when(alertRepository.findAlerts(
            any(Integer.class),
            any(com.aegis.domain.AlertStatus.class),
            anyLong(),
            anyLong(),
            anyInt(),
            anyInt()
        )).thenReturn(mockAlerts);
        
        // Create controller with alert repository
        AegisGraphQLController controllerWithAlerts = new AegisGraphQLController(
            aqlTranspiler, queryExecutor, alertRepository);
        
        // Act
        List<com.aegis.domain.Alert> result = controllerWithAlerts.getAlerts(
            severity, status, timeRange, limit, offset);
        
        // Assert
        assertThat(result).isNotNull();
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getId()).isEqualTo("alert-1");
        assertThat(result.get(1).getId()).isEqualTo("alert-2");
        
        // Verify repository was called with correct parameters
        verify(alertRepository, times(1)).findAlerts(
            eq(4),
            eq(com.aegis.domain.AlertStatus.OPEN),
            anyLong(),
            anyLong(),
            eq(100),
            eq(0)
        );
    }
    
    @Test
    void testGetAlerts_WithNullFilters() {
        // Arrange
        Integer severity = null;
        String status = null;
        String timeRange = "last 7d";
        Integer limit = 50;
        Integer offset = 0;
        
        List<com.aegis.domain.Alert> mockAlerts = new ArrayList<>();
        when(alertRepository.findAlerts(
            isNull(),
            isNull(),
            anyLong(),
            anyLong(),
            anyInt(),
            anyInt()
        )).thenReturn(mockAlerts);
        
        AegisGraphQLController controllerWithAlerts = new AegisGraphQLController(
            aqlTranspiler, queryExecutor, alertRepository);
        
        // Act
        List<com.aegis.domain.Alert> result = controllerWithAlerts.getAlerts(
            severity, status, timeRange, limit, offset);
        
        // Assert
        assertThat(result).isNotNull();
        assertThat(result).isEmpty();
        
        verify(alertRepository, times(1)).findAlerts(
            isNull(),
            isNull(),
            anyLong(),
            anyLong(),
            eq(50),
            eq(0)
        );
    }
    
    @Test
    void testGetAlerts_InvalidStatus() {
        // Arrange
        Integer severity = 3;
        String status = "INVALID_STATUS";
        String timeRange = "last 24h";
        Integer limit = 100;
        Integer offset = 0;
        
        AegisGraphQLController controllerWithAlerts = new AegisGraphQLController(
            aqlTranspiler, queryExecutor, alertRepository);
        
        // Act & Assert
        assertThatThrownBy(() -> controllerWithAlerts.getAlerts(
            severity, status, timeRange, limit, offset))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid alert status");
        
        verify(alertRepository, never()).findAlerts(
            any(), any(), anyLong(), anyLong(), anyInt(), anyInt());
    }
    
    @Test
    void testGetAlerts_InvalidTimeRange() {
        // Arrange
        Integer severity = 3;
        String status = "OPEN";
        String timeRange = "invalid time";
        Integer limit = 100;
        Integer offset = 0;
        
        AegisGraphQLController controllerWithAlerts = new AegisGraphQLController(
            aqlTranspiler, queryExecutor, alertRepository);
        
        // Act & Assert
        assertThatThrownBy(() -> controllerWithAlerts.getAlerts(
            severity, status, timeRange, limit, offset))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid time range format");
        
        verify(alertRepository, never()).findAlerts(
            any(), any(), anyLong(), anyLong(), anyInt(), anyInt());
    }
    
    @Test
    void testGetAlerts_EmptyTimeRange() {
        // Arrange
        Integer severity = 3;
        String status = "OPEN";
        String timeRange = "";
        Integer limit = 100;
        Integer offset = 0;
        
        AegisGraphQLController controllerWithAlerts = new AegisGraphQLController(
            aqlTranspiler, queryExecutor, alertRepository);
        
        // Act & Assert
        assertThatThrownBy(() -> controllerWithAlerts.getAlerts(
            severity, status, timeRange, limit, offset))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("timeRange is required");
        
        verify(alertRepository, never()).findAlerts(
            any(), any(), anyLong(), anyLong(), anyInt(), anyInt());
    }
    
    @Test
    void testGetAlerts_LimitExceedsMaximum() {
        // Arrange
        Integer severity = 3;
        String status = "OPEN";
        String timeRange = "last 24h";
        Integer limit = 50000; // Exceeds max of 10000
        Integer offset = 0;
        
        List<com.aegis.domain.Alert> mockAlerts = new ArrayList<>();
        when(alertRepository.findAlerts(
            any(), any(), anyLong(), anyLong(), anyInt(), anyInt()
        )).thenReturn(mockAlerts);
        
        AegisGraphQLController controllerWithAlerts = new AegisGraphQLController(
            aqlTranspiler, queryExecutor, alertRepository);
        
        // Act
        List<com.aegis.domain.Alert> result = controllerWithAlerts.getAlerts(
            severity, status, timeRange, limit, offset);
        
        // Assert - limit should be capped at 10000
        verify(alertRepository, times(1)).findAlerts(
            any(), any(), anyLong(), anyLong(), eq(10000), eq(0));
    }
    
    @Test
    void testGetAlerts_WithPagination() {
        // Arrange
        Integer severity = null;
        String status = null;
        String timeRange = "last 7d";
        Integer limit = 20;
        Integer offset = 40;
        
        List<com.aegis.domain.Alert> mockAlerts = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            com.aegis.domain.Alert alert = new com.aegis.domain.Alert();
            alert.setId("alert-" + (40 + i));
            alert.setSeverity(3);
            mockAlerts.add(alert);
        }
        
        when(alertRepository.findAlerts(
            any(), any(), anyLong(), anyLong(), anyInt(), anyInt()
        )).thenReturn(mockAlerts);
        
        AegisGraphQLController controllerWithAlerts = new AegisGraphQLController(
            aqlTranspiler, queryExecutor, alertRepository);
        
        // Act
        List<com.aegis.domain.Alert> result = controllerWithAlerts.getAlerts(
            severity, status, timeRange, limit, offset);
        
        // Assert
        assertThat(result).hasSize(20);
        assertThat(result.get(0).getId()).isEqualTo("alert-40");
        
        verify(alertRepository, times(1)).findAlerts(
            isNull(), isNull(), anyLong(), anyLong(), eq(20), eq(40));
    }
    
    @Test
    void testGetAlerts_AllSeverityLevels() {
        // Test each severity level
        for (int sev = 1; sev <= 5; sev++) {
            // Arrange
            Integer severity = sev;
            String status = null;
            String timeRange = "last 24h";
            Integer limit = 100;
            Integer offset = 0;
            
            List<com.aegis.domain.Alert> mockAlerts = new ArrayList<>();
            com.aegis.domain.Alert alert = new com.aegis.domain.Alert();
            alert.setId("alert-sev-" + sev);
            alert.setSeverity(sev);
            mockAlerts.add(alert);
            
            when(alertRepository.findAlerts(
                eq(sev), any(), anyLong(), anyLong(), anyInt(), anyInt()
            )).thenReturn(mockAlerts);
            
            AegisGraphQLController controllerWithAlerts = new AegisGraphQLController(
                aqlTranspiler, queryExecutor, alertRepository);
            
            // Act
            List<com.aegis.domain.Alert> result = controllerWithAlerts.getAlerts(
                severity, status, timeRange, limit, offset);
            
            // Assert
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getSeverity()).isEqualTo(sev);
        }
    }
    
    @Test
    void testGetAlerts_AllStatusTypes() {
        // Test each status type
        String[] statuses = {"OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "RESOLVED", "FALSE_POSITIVE"};
        
        for (String statusStr : statuses) {
            // Arrange
            Integer severity = null;
            String timeRange = "last 24h";
            Integer limit = 100;
            Integer offset = 0;
            
            List<com.aegis.domain.Alert> mockAlerts = new ArrayList<>();
            com.aegis.domain.Alert alert = new com.aegis.domain.Alert();
            alert.setId("alert-" + statusStr);
            alert.setStatus(com.aegis.domain.AlertStatus.valueOf(statusStr));
            mockAlerts.add(alert);
            
            when(alertRepository.findAlerts(
                any(), eq(com.aegis.domain.AlertStatus.valueOf(statusStr)),
                anyLong(), anyLong(), anyInt(), anyInt()
            )).thenReturn(mockAlerts);
            
            AegisGraphQLController controllerWithAlerts = new AegisGraphQLController(
                aqlTranspiler, queryExecutor, alertRepository);
            
            // Act
            List<com.aegis.domain.Alert> result = controllerWithAlerts.getAlerts(
                severity, statusStr, timeRange, limit, offset);
            
            // Assert
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getStatus().name()).isEqualTo(statusStr);
        }
    }
    
    @Test
    void testGetAlerts_TimeRangeFormats() {
        // Test various time range formats
        String[] timeRanges = {"last 1h", "last 24h", "last 7d", "last 4w", "last 3m", "last 1y"};
        
        for (String timeRange : timeRanges) {
            // Arrange
            List<com.aegis.domain.Alert> mockAlerts = new ArrayList<>();
            when(alertRepository.findAlerts(
                any(), any(), anyLong(), anyLong(), anyInt(), anyInt()
            )).thenReturn(mockAlerts);
            
            AegisGraphQLController controllerWithAlerts = new AegisGraphQLController(
                aqlTranspiler, queryExecutor, alertRepository);
            
            // Act
            List<com.aegis.domain.Alert> result = controllerWithAlerts.getAlerts(
                null, null, timeRange, 100, 0);
            
            // Assert - should not throw exception
            assertThat(result).isNotNull();
        }
    }
}
