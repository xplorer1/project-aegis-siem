package com.aegis.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Service for producing messages to Kafka topics with error handling
 * Wraps KafkaTemplate to provide a consistent interface for event production
 */
@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Sends a message to the specified Kafka topic
     * 
     * @param topic the Kafka topic to send to
     * @param key the message key (for partitioning)
     * @param value the message value
     * @return CompletableFuture with the send result
     */
    public CompletableFuture<SendResult<String, Object>> send(String topic, String key, Object value) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, value);
        
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send message to topic {}: {}", topic, ex.getMessage(), ex);
            } else {
                log.debug("Successfully sent message to topic {} partition {} offset {}",
                    topic,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            }
        });
        
        return future;
    }

    /**
     * Sends a message to the specified Kafka topic without a key
     * 
     * @param topic the Kafka topic to send to
     * @param value the message value
     * @return CompletableFuture with the send result
     */
    public CompletableFuture<SendResult<String, Object>> send(String topic, Object value) {
        return send(topic, null, value);
    }
}
