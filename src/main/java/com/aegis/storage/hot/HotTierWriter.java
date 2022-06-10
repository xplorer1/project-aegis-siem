package com.aegis.storage.hot;

import com.aegis.domain.OcsfEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes normalized events to OpenSearch hot tier storage
 * Consumes events from Kafka and indexes them in OpenSearch
 */
@Service
public class HotTierWriter {
    private static final Logger logger = LoggerFactory.getLogger(HotTierWriter.class);
    
    @Autowired
    private RestHighLevelClient client;
    
    @Autowired
    private ObjectMapper objectMapper;
    
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
     * Index an event to OpenSearch
     * Placeholder - will be implemented in next task
     * 
     * @param event The event to index
     */
    private void index(OcsfEvent event) {
        // Placeholder - implementation in next task
        logger.debug("Indexing event: {}", event.getClassName());
    }
}
