package com.aegis.storage.hot;

import com.aegis.domain.OcsfEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Writes normalized events to OpenSearch hot tier storage
 * Consumes events from Kafka and indexes them in OpenSearch using bulk API
 */
@Service
public class HotTierWriter {
    private static final Logger logger = LoggerFactory.getLogger(HotTierWriter.class);
    private static final int BULK_SIZE = 1000;
    private static final int BULK_FLUSH_INTERVAL_MS = 5000;
    
    @Autowired
    private RestHighLevelClient client;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private IndexingMetrics metrics;
    
    private BulkProcessor bulkProcessor;
    
    /**
     * Initialize bulk processor on startup
     */
    @javax.annotation.PostConstruct
    public void init() {
        bulkProcessor = BulkProcessor.builder(
            (request, bulkListener) -> 
                client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    logger.debug("Executing bulk request {} with {} actions", 
                        executionId, request.numberOfActions());
                }
                
                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    // Record bulk latency
                    metrics.recordBulkLatency(response.getTook().millis());
                    
                    if (response.hasFailures()) {
                        logger.error("Bulk request {} had failures: {}", 
                            executionId, response.buildFailureMessage());
                        
                        // Count successful and failed documents
                        int successCount = 0;
                        int failureCount = 0;
                        for (var item : response.getItems()) {
                            if (item.isFailed()) {
                                failureCount++;
                                metrics.recordIndexingError();
                            } else {
                                successCount++;
                            }
                        }
                        
                        metrics.recordDocumentsIndexed(successCount);
                        logger.info("Bulk request {}: {} succeeded, {} failed", 
                            executionId, successCount, failureCount);
                        
                        // Handle partial failures - retry failed documents
                        handlePartialFailures(request, response);
                    } else {
                        // All documents indexed successfully
                        metrics.recordDocumentsIndexed(response.getItems().length);
                        logger.debug("Bulk request {} completed successfully in {} ms", 
                            executionId, response.getTook().millis());
                    }
                }
                
                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    logger.error("Bulk request {} failed", executionId, failure);
                }
            })
            .setBulkActions(BULK_SIZE)
            .setFlushInterval(org.opensearch.core.common.unit.TimeValue.timeValueMillis(BULK_FLUSH_INTERVAL_MS))
            .setConcurrentRequests(1)
            .setBackoffPolicy(
                org.opensearch.action.bulk.BackoffPolicy.exponentialBackoff(
                    org.opensearch.core.common.unit.TimeValue.timeValueMillis(100), 3))
            .build();
        
        logger.info("Bulk processor initialized: size={}, flush_interval={}ms", 
            BULK_SIZE, BULK_FLUSH_INTERVAL_MS);
    }
    
    /**
     * Clean up bulk processor on shutdown
     */
    @javax.annotation.PreDestroy
    public void cleanup() {
        if (bulkProcessor != null) {
            try {
                bulkProcessor.flush();
                bulkProcessor.close();
                logger.info("Bulk processor closed");
            } catch (Exception e) {
                logger.error("Error closing bulk processor", e);
            }
        }
    }
    
    /**
     * Consume normalized events from Kafka and index to OpenSearch
     * 
     * @param event The normalized OCSF event
     */
    @KafkaListener(
        topics = "${aegis.kafka.topics.normalized-events:normalized-events}",
        groupId = "${aegis.kafka.consumer.group-id:aegis-hot-tier-writer}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEvent(OcsfEvent event) {
        try {
            logger.debug("Received event for indexing: {}", event.getClassName());
            
            // Index the event (will be implemented in next task)
            index(event);
            
        } catch (Exception e) {
            logger.error("Failed to process event for hot tier storage", e);
            // TODO: Send to dead letter queue
        }
    }
    
    /**
     * Index an event to OpenSearch using bulk processor
     * Automatically rotates indices daily based on event timestamp
     * 
     * @param event The event to index
     */
    private void index(OcsfEvent event) {
        try {
            // Generate index name with date - provides automatic daily rotation
            String indexName = generateIndexName(event.getTime());
            
            // Convert event to JSON
            String json = objectMapper.writeValueAsString(event);
            
            // Create index request
            IndexRequest request = new IndexRequest(indexName)
                .id(UUID.randomUUID().toString())
                .source(json, XContentType.JSON);
            
            // Add to bulk processor for batched indexing
            bulkProcessor.add(request);
            
            logger.trace("Added event to bulk processor for index: {}", indexName);
            
        } catch (Exception e) {
            logger.error("Failed to add event to bulk processor", e);
            throw new RuntimeException("Event indexing failed", e);
        }
    }
    
    /**
     * Generate index name with date suffix for daily rotation
     * Format: aegis-events-YYYY-MM-DD
     * 
     * This provides automatic daily index rotation:
     * - Events are indexed to date-specific indices
     * - New indices are created automatically each day
     * - Enables efficient time-based queries and retention management
     * 
     * @param timestamp Event timestamp in milliseconds
     * @return Index name with date suffix
     */
    private String generateIndexName(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        String date = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withZone(ZoneOffset.UTC)
            .format(instant);
        return "aegis-events-" + date;
    }
    
    /**
     * Handle partial failures in bulk response
     * Retry failed documents up to 3 times
     * 
     * @param request Original bulk request
     * @param response Bulk response with failures
     */
    private void handlePartialFailures(BulkRequest request, BulkResponse response) {
        for (int i = 0; i < response.getItems().length; i++) {
            var item = response.getItems()[i];
            
            if (item.isFailed()) {
                var failure = item.getFailure();
                logger.warn("Failed to index document {}: {}", 
                    item.getId(), failure.getMessage());
                
                // Check if error is retryable
                if (isRetryable(failure)) {
                    // Get original request
                    var originalRequest = request.requests().get(i);
                    
                    if (originalRequest instanceof IndexRequest) {
                        retryIndexRequest((IndexRequest) originalRequest);
                    }
                } else {
                    logger.error("Non-retryable error for document {}: {}", 
                        item.getId(), failure.getMessage());
                    // TODO: Send to dead letter queue
                }
            }
        }
    }
    
    /**
     * Check if a failure is retryable
     */
    private boolean isRetryable(org.opensearch.action.bulk.BulkItemResponse.Failure failure) {
        String message = failure.getMessage().toLowerCase();
        
        // Retryable errors
        return message.contains("timeout") ||
               message.contains("unavailable") ||
               message.contains("too many requests") ||
               message.contains("circuit breaker");
    }
    
    /**
     * Retry a failed index request
     * Uses exponential backoff with max 3 retries
     */
    private void retryIndexRequest(IndexRequest request) {
        int maxRetries = 3;
        int retryDelay = 100; // ms
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                metrics.recordRetry();
                Thread.sleep(retryDelay * attempt);
                
                var response = client.index(request, RequestOptions.DEFAULT);
                metrics.recordDocumentIndexed();
                logger.info("Successfully retried document {} on attempt {}", 
                    response.getId(), attempt);
                return;
                
            } catch (Exception e) {
                logger.warn("Retry attempt {} failed for document: {}", 
                    attempt, e.getMessage());
                
                if (attempt == maxRetries) {
                    metrics.recordIndexingError();
                    logger.error("Max retries exceeded for document", e);
                    // TODO: Send to dead letter queue
                }
            }
        }
    }
}
