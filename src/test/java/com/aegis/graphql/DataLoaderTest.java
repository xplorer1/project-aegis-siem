package com.aegis.graphql;

import com.aegis.domain.Alert;
import com.aegis.domain.AlertStatus;
import com.aegis.domain.OcsfEvent;
import com.aegis.storage.hot.AlertRepository;
import com.aegis.storage.hot.OpenSearchRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.graphql.GraphQlTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.graphql.test.tester.GraphQlTester;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Tests for GraphQL DataLoader functionality.
 * 
 * These tests verify that DataLoader correctly batches queries to prevent N+1 problems
 * when fetching related entities (e.g., events for alerts).
 */
@GraphQlTest(AegisGraphQLController.class)
class DataLoaderTest {
    
    @Autowired
    private GraphQlTester graphQlTester;
    
    @MockBean
    private AlertRepository alertRepository;
    
    @MockBean
    private OpenSearchRepository eventRepository;
    
    @MockBean
    private com.aegis.query.AqlTranspiler aqlTranspiler;
    
    @MockBean
    private com.aegis.query.QueryExecutor queryExecutor;
    
    private List<Alert> testAlerts;
    private List<OcsfEvent> testEvents;
    
    @BeforeEach
    void setUp() {
        // Create test alerts with event IDs
        testAlerts = new ArrayList<>();
        testEvents = new ArrayList<>();
        
        // Alert 1 with 3 events
        Alert alert1 = new Alert();
        alert1.setId("alert-1");
        alert1.setTime(Instant.now());
        alert1.setTenantId("tenant-1");
        alert1.setSeverity(4);
        alert1.setTitle("Brute Force Attack");
        alert1.setDescription("Multiple failed login attempts detected");
        alert1.setRuleId("rule-1");
        alert1.setRuleName("Brute Force Detection");
        alert1.setEventIds(Arrays.asList("event-1", "event-2", "event-3"));
        alert1.setStatus(AlertStatus.OPEN);
        testAlerts.add(alert1);
        
        // Alert 2 with 2 events
        Alert alert2 = new Alert();
        alert2.setId("alert-2");
        alert2.setTime(Instant.now());
        alert2.setTenantId("tenant-1");
        alert2.setSeverity(3);
        alert2.setTitle("Port Scan Detected");
        alert2.setDescription("Scanning activity from suspicious IP");
        alert2.setRuleId("rule-2");
        alert2.setRuleName("Port Scan Detection");
        alert2.setEventIds(Arrays.asList("event-4", "event-5"));
        alert2.setStatus(AlertStatus.OPEN);
        testAlerts.add(alert2);
        
        // Alert 3 with 2 events (one overlapping with alert 1)
        Alert alert3 = new Alert();
        alert3.setId("alert-3");
        alert3.setTime(Instant.now());
        alert3.setTenantId("tenant-1");
        alert3.setSeverity(5);
        alert3.setTitle("Data Exfiltration");
        alert3.setDescription("Large data transfer to external IP");
        alert3.setRuleId("rule-3");
        alert3.setRuleName("Data Exfiltration Detection");
        alert3.setEventIds(Arrays.asList("event-3", "event-6"));
        alert3.setStatus(AlertStatus.OPEN);
        testAlerts.add(alert3);
        
        // Create test events
        for (int i = 1; i <= 6; i++) {
            OcsfEvent event = new OcsfEvent();
            event.setUuid("event-" + i);
            event.setTime(Instant.now());
            event.setTenantId("tenant-1");
            event.setSeverity(3);
            event.setClassUid(3001);
            event.setCategoryUid(3);
            testEvents.add(event);
        }
    }
    
    @Test
    @DisplayName("DataLoader should batch event fetches for multiple alerts")
    void testDataLoaderBatchesEventFetches() {
        // Setup mock responses
        when(alertRepository.findAlerts(any(), any(), anyLong(), anyLong(), anyInt(), anyInt()))
            .thenReturn(testAlerts);
        
        // Mock the batch fetch - DataLoader will call this once with all unique event IDs
        when(eventRepository.findByIds(any()))
            .thenAnswer(invocation -> {
                List<String> requestedIds = invocation.getArgument(0);
                List<OcsfEvent> result = new ArrayList<>();
                for (String id : requestedIds) {
                    int index = Integer.parseInt(id.split("-")[1]) - 1;
                    if (index >= 0 && index < testEvents.size()) {
                        result.add(testEvents.get(index));
                    }
                }
                return result;
            });
        
        // Execute GraphQL query that requests alerts with their events
        String query = """
            query {
              getAlerts(timeRange: "last 24h") {
                id
                title
                eventIds
                events {
                  id
                  time
                  severity
                }
              }
            }
            """;
        
        graphQlTester.document(query)
            .execute()
            .path("getAlerts")
            .entityList(Alert.class)
            .hasSize(3);
        
        // Verify that findByIds was called (DataLoader batching)
        // It should be called once or a small number of times, NOT once per alert
        ArgumentCaptor<List<String>> idsCaptor = ArgumentCaptor.forClass(List.class);
        verify(eventRepository, atLeastOnce()).findByIds(idsCaptor.capture());
        
        // Verify that we didn't make individual queries for each event
        // The total number of unique event IDs is 6, so we should fetch them in batch(es)
        List<List<String>> allCalls = idsCaptor.getAllValues();
        int totalFetches = allCalls.size();
        
        // With DataLoader, we should have significantly fewer calls than the number of alerts
        // Ideally 1 batch call, but could be a few depending on timing
        assertThat(totalFetches).isLessThanOrEqualTo(3)
            .withFailMessage("Expected DataLoader to batch fetches, but got %d separate calls", totalFetches);
        
        // Verify that all unique event IDs were requested
        List<String> allRequestedIds = new ArrayList<>();
        for (List<String> call : allCalls) {
            allRequestedIds.addAll(call);
        }
        
        // We have 6 unique event IDs across all alerts (event-1 through event-6)
        // Note: event-3 appears in both alert-1 and alert-3, but should only be fetched once
        assertThat(allRequestedIds).containsExactlyInAnyOrder(
            "event-1", "event-2", "event-3", "event-4", "event-5", "event-6"
        );
    }
    
    @Test
    @DisplayName("DataLoader should handle alerts with no events")
    void testDataLoaderHandlesEmptyEventIds() {
        // Create alert with no events
        Alert alertNoEvents = new Alert();
        alertNoEvents.setId("alert-empty");
        alertNoEvents.setTime(Instant.now());
        alertNoEvents.setTenantId("tenant-1");
        alertNoEvents.setSeverity(2);
        alertNoEvents.setTitle("Test Alert");
        alertNoEvents.setDescription("Alert with no events");
        alertNoEvents.setRuleId("rule-test");
        alertNoEvents.setRuleName("Test Rule");
        alertNoEvents.setEventIds(new ArrayList<>());
        alertNoEvents.setStatus(AlertStatus.OPEN);
        
        when(alertRepository.findAlerts(any(), any(), anyLong(), anyLong(), anyInt(), anyInt()))
            .thenReturn(Arrays.asList(alertNoEvents));
        
        String query = """
            query {
              getAlerts(timeRange: "last 24h") {
                id
                title
                events {
                  id
                }
              }
            }
            """;
        
        graphQlTester.document(query)
            .execute()
            .path("getAlerts[0].events")
            .entityList(OcsfEvent.class)
            .hasSize(0);
        
        // Verify that no event fetch was attempted
        verify(eventRepository, never()).findByIds(any());
    }
    
    @Test
    @DisplayName("DataLoader should handle null events gracefully")
    void testDataLoaderHandlesNullEvents() {
        // Create alert with event IDs where some events don't exist
        Alert alert = new Alert();
        alert.setId("alert-missing");
        alert.setTime(Instant.now());
        alert.setTenantId("tenant-1");
        alert.setSeverity(3);
        alert.setTitle("Test Alert");
        alert.setDescription("Alert with missing events");
        alert.setRuleId("rule-test");
        alert.setRuleName("Test Rule");
        alert.setEventIds(Arrays.asList("event-1", "event-missing", "event-2"));
        alert.setStatus(AlertStatus.OPEN);
        
        when(alertRepository.findAlerts(any(), any(), anyLong(), anyLong(), anyInt(), anyInt()))
            .thenReturn(Arrays.asList(alert));
        
        // Mock findByIds to return null for missing event
        when(eventRepository.findByIds(any()))
            .thenAnswer(invocation -> {
                List<String> requestedIds = invocation.getArgument(0);
                List<OcsfEvent> result = new ArrayList<>();
                for (String id : requestedIds) {
                    if (id.equals("event-1")) {
                        result.add(testEvents.get(0));
                    } else if (id.equals("event-2")) {
                        result.add(testEvents.get(1));
                    } else {
                        result.add(null); // Missing event
                    }
                }
                return result;
            });
        
        String query = """
            query {
              getAlerts(timeRange: "last 24h") {
                id
                events {
                  id
                }
              }
            }
            """;
        
        graphQlTester.document(query)
            .execute()
            .path("getAlerts[0].events")
            .entityList(OcsfEvent.class)
            .hasSize(2); // Should only return the 2 found events, filtering out null
    }
    
    @Test
    @DisplayName("DataLoader should deduplicate event IDs across alerts")
    void testDataLoaderDeduplicatesEventIds() {
        // Both alerts reference the same event
        Alert alert1 = new Alert();
        alert1.setId("alert-1");
        alert1.setTime(Instant.now());
        alert1.setTenantId("tenant-1");
        alert1.setSeverity(4);
        alert1.setTitle("Alert 1");
        alert1.setDescription("First alert");
        alert1.setRuleId("rule-1");
        alert1.setRuleName("Rule 1");
        alert1.setEventIds(Arrays.asList("event-1", "event-2"));
        alert1.setStatus(AlertStatus.OPEN);
        
        Alert alert2 = new Alert();
        alert2.setId("alert-2");
        alert2.setTime(Instant.now());
        alert2.setTenantId("tenant-1");
        alert2.setSeverity(4);
        alert2.setTitle("Alert 2");
        alert2.setDescription("Second alert");
        alert2.setRuleId("rule-2");
        alert2.setRuleName("Rule 2");
        alert2.setEventIds(Arrays.asList("event-2", "event-3")); // event-2 is shared
        alert2.setStatus(AlertStatus.OPEN);
        
        when(alertRepository.findAlerts(any(), any(), anyLong(), anyLong(), anyInt(), anyInt()))
            .thenReturn(Arrays.asList(alert1, alert2));
        
        when(eventRepository.findByIds(any()))
            .thenAnswer(invocation -> {
                List<String> requestedIds = invocation.getArgument(0);
                List<OcsfEvent> result = new ArrayList<>();
                for (String id : requestedIds) {
                    int index = Integer.parseInt(id.split("-")[1]) - 1;
                    if (index >= 0 && index < testEvents.size()) {
                        result.add(testEvents.get(index));
                    }
                }
                return result;
            });
        
        String query = """
            query {
              getAlerts(timeRange: "last 24h") {
                id
                events {
                  id
                }
              }
            }
            """;
        
        graphQlTester.document(query)
            .execute()
            .path("getAlerts")
            .entityList(Alert.class)
            .hasSize(2);
        
        // Verify that event-2 was only fetched once despite being in both alerts
        ArgumentCaptor<List<String>> idsCaptor = ArgumentCaptor.forClass(List.class);
        verify(eventRepository, atLeastOnce()).findByIds(idsCaptor.capture());
        
        List<String> allRequestedIds = new ArrayList<>();
        for (List<String> call : idsCaptor.getAllValues()) {
            allRequestedIds.addAll(call);
        }
        
        // Should have exactly 3 unique event IDs
        assertThat(allRequestedIds).containsExactlyInAnyOrder("event-1", "event-2", "event-3");
    }
}
