package com.aegis.storage.hot;

import com.aegis.domain.OcsfEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for OpenSearchRepository batch fetch functionality.
 * 
 * Tests the findByIds method which is used by DataLoader to batch fetch events.
 */
@ExtendWith(MockitoExtension.class)
class OpenSearchRepositoryBatchTest {
    
    @Mock
    private RestHighLevelClient client;
    
    @Mock
    private ObjectMapper objectMapper;
    
    @InjectMocks
    private OpenSearchRepository repository;
    
    @Mock
    private SearchResponse searchResponse;
    
    @Mock
    private SearchHits searchHits;
    
    @BeforeEach
    void setUp() {
        // Common setup for mocks
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchResponse.getTook()).thenReturn(TimeValue.timeValueMillis(10));
    }
    
    @Test
    @DisplayName("findByIds should fetch events in correct order")
    void testFindByIdsReturnsEventsInOrder() throws Exception {
        // Arrange
        List<String> requestedIds = Arrays.asList("event-1", "event-2", "event-3");
        
        // Create mock events
        OcsfEvent event1 = createEvent("event-1");
        OcsfEvent event2 = createEvent("event-2");
        OcsfEvent event3 = createEvent("event-3");
        
        // Create mock search hits (returned in different order than requested)
        SearchHit hit1 = mock(SearchHit.class);
        when(hit1.getId()).thenReturn("event-2");
        when(hit1.getSourceAsString()).thenReturn("{\"uuid\":\"event-2\"}");
        
        SearchHit hit2 = mock(SearchHit.class);
        when(hit2.getId()).thenReturn("event-1");
        when(hit2.getSourceAsString()).thenReturn("{\"uuid\":\"event-1\"}");
        
        SearchHit hit3 = mock(SearchHit.class);
        when(hit3.getId()).thenReturn("event-3");
        when(hit3.getSourceAsString()).thenReturn("{\"uuid\":\"event-3\"}");
        
        SearchHit[] hits = new SearchHit[]{hit1, hit2, hit3};
        when(searchHits.getHits()).thenReturn(hits);
        
        when(client.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(searchResponse);
        
        when(objectMapper.readValue("{\"uuid\":\"event-1\"}", OcsfEvent.class)).thenReturn(event1);
        when(objectMapper.readValue("{\"uuid\":\"event-2\"}", OcsfEvent.class)).thenReturn(event2);
        when(objectMapper.readValue("{\"uuid\":\"event-3\"}", OcsfEvent.class)).thenReturn(event3);
        
        // Act
        List<OcsfEvent> results = repository.findByIds(requestedIds);
        
        // Assert
        assertThat(results).hasSize(3);
        assertThat(results.get(0).getUuid()).isEqualTo("event-1");
        assertThat(results.get(1).getUuid()).isEqualTo("event-2");
        assertThat(results.get(2).getUuid()).isEqualTo("event-3");
        
        // Verify that search was called with IDs query
        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(client).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));
        
        SearchRequest capturedRequest = requestCaptor.getValue();
        assertThat(capturedRequest.indices()).containsExactly("aegis-events-*");
    }
    
    @Test
    @DisplayName("findByIds should handle missing events")
    void testFindByIdsHandlesMissingEvents() throws Exception {
        // Arrange
        List<String> requestedIds = Arrays.asList("event-1", "event-missing", "event-2");
        
        OcsfEvent event1 = createEvent("event-1");
        OcsfEvent event2 = createEvent("event-2");
        
        // Only return 2 hits (event-missing is not found)
        SearchHit hit1 = mock(SearchHit.class);
        when(hit1.getId()).thenReturn("event-1");
        when(hit1.getSourceAsString()).thenReturn("{\"uuid\":\"event-1\"}");
        
        SearchHit hit2 = mock(SearchHit.class);
        when(hit2.getId()).thenReturn("event-2");
        when(hit2.getSourceAsString()).thenReturn("{\"uuid\":\"event-2\"}");
        
        SearchHit[] hits = new SearchHit[]{hit1, hit2};
        when(searchHits.getHits()).thenReturn(hits);
        
        when(client.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(searchResponse);
        
        when(objectMapper.readValue("{\"uuid\":\"event-1\"}", OcsfEvent.class)).thenReturn(event1);
        when(objectMapper.readValue("{\"uuid\":\"event-2\"}", OcsfEvent.class)).thenReturn(event2);
        
        // Act
        List<OcsfEvent> results = repository.findByIds(requestedIds);
        
        // Assert
        assertThat(results).hasSize(3);
        assertThat(results.get(0)).isEqualTo(event1);
        assertThat(results.get(1)).isNull(); // Missing event
        assertThat(results.get(2)).isEqualTo(event2);
    }
    
    @Test
    @DisplayName("findByIds should handle empty input")
    void testFindByIdsHandlesEmptyInput() {
        // Act
        List<OcsfEvent> results = repository.findByIds(Arrays.asList());
        
        // Assert
        assertThat(results).isEmpty();
        verify(client, never()).search(any(), any());
    }
    
    @Test
    @DisplayName("findByIds should handle null input")
    void testFindByIdsHandlesNullInput() {
        // Act
        List<OcsfEvent> results = repository.findByIds(null);
        
        // Assert
        assertThat(results).isEmpty();
        verify(client, never()).search(any(), any());
    }
    
    @Test
    @DisplayName("findByIds should batch large number of IDs")
    void testFindByIdsBatchesLargeRequest() throws Exception {
        // Arrange - request 100 event IDs
        List<String> requestedIds = new java.util.ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            requestedIds.add("event-" + i);
        }
        
        // Create mock hits for all events
        SearchHit[] hits = new SearchHit[100];
        for (int i = 0; i < 100; i++) {
            SearchHit hit = mock(SearchHit.class);
            String id = "event-" + (i + 1);
            when(hit.getId()).thenReturn(id);
            when(hit.getSourceAsString()).thenReturn("{\"uuid\":\"" + id + "\"}");
            hits[i] = hit;
            
            OcsfEvent event = createEvent(id);
            when(objectMapper.readValue("{\"uuid\":\"" + id + "\"}", OcsfEvent.class))
                .thenReturn(event);
        }
        
        when(searchHits.getHits()).thenReturn(hits);
        when(client.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(searchResponse);
        
        // Act
        List<OcsfEvent> results = repository.findByIds(requestedIds);
        
        // Assert
        assertThat(results).hasSize(100);
        
        // Verify all events are in correct order
        for (int i = 0; i < 100; i++) {
            assertThat(results.get(i).getUuid()).isEqualTo("event-" + (i + 1));
        }
        
        // Verify only one search call was made (batch fetch)
        verify(client, times(1)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));
    }
    
    @Test
    @DisplayName("findByIds should handle duplicate IDs")
    void testFindByIdsHandlesDuplicateIds() throws Exception {
        // Arrange - request same ID multiple times
        List<String> requestedIds = Arrays.asList("event-1", "event-2", "event-1", "event-3", "event-2");
        
        OcsfEvent event1 = createEvent("event-1");
        OcsfEvent event2 = createEvent("event-2");
        OcsfEvent event3 = createEvent("event-3");
        
        SearchHit hit1 = mock(SearchHit.class);
        when(hit1.getId()).thenReturn("event-1");
        when(hit1.getSourceAsString()).thenReturn("{\"uuid\":\"event-1\"}");
        
        SearchHit hit2 = mock(SearchHit.class);
        when(hit2.getId()).thenReturn("event-2");
        when(hit2.getSourceAsString()).thenReturn("{\"uuid\":\"event-2\"}");
        
        SearchHit hit3 = mock(SearchHit.class);
        when(hit3.getId()).thenReturn("event-3");
        when(hit3.getSourceAsString()).thenReturn("{\"uuid\":\"event-3\"}");
        
        SearchHit[] hits = new SearchHit[]{hit1, hit2, hit3};
        when(searchHits.getHits()).thenReturn(hits);
        
        when(client.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(searchResponse);
        
        when(objectMapper.readValue("{\"uuid\":\"event-1\"}", OcsfEvent.class)).thenReturn(event1);
        when(objectMapper.readValue("{\"uuid\":\"event-2\"}", OcsfEvent.class)).thenReturn(event2);
        when(objectMapper.readValue("{\"uuid\":\"event-3\"}", OcsfEvent.class)).thenReturn(event3);
        
        // Act
        List<OcsfEvent> results = repository.findByIds(requestedIds);
        
        // Assert - should return events in requested order, including duplicates
        assertThat(results).hasSize(5);
        assertThat(results.get(0).getUuid()).isEqualTo("event-1");
        assertThat(results.get(1).getUuid()).isEqualTo("event-2");
        assertThat(results.get(2).getUuid()).isEqualTo("event-1"); // Duplicate
        assertThat(results.get(3).getUuid()).isEqualTo("event-3");
        assertThat(results.get(4).getUuid()).isEqualTo("event-2"); // Duplicate
    }
    
    /**
     * Helper method to create a test event
     */
    private OcsfEvent createEvent(String id) {
        OcsfEvent event = new OcsfEvent();
        event.setUuid(id);
        event.setTime(Instant.now());
        event.setTenantId("tenant-1");
        event.setSeverity(3);
        event.setClassUid(3001);
        event.setCategoryUid(3);
        return event;
    }
}
