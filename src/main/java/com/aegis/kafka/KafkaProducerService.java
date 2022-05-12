package com.aegis.kafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
    private final Counter sendSuccessCounter;
    private final Counter sendFailureCounter;
    private final Timer sendLatencyTimer;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate, MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        
        // Initialize metrics
        this.sendSuccessCounter = Counter.builder("kafka.producer.send.success")
            .description("Number of successfully sent messages")
            .register(meterRegistry);
        
        this.sendFailureCounter = Counter.builder("kafka.producer.send.failure")
            .description("Number of failed message sends")
            .register(meterRegistry);
        
        this.sendLatencyTimer = Timer.builder("kafka.producer.send.latency")
            .description("Latency of message sends")
            .register(meterRegistry);
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
        Timer.Sample sample = Timer.start();
        
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, value);
        
        future.whenComplete((result, ex) -> {
            sample.stop(sendLatencyTimer);
            
            if (ex != null) {
                sendFailureCounter.increment();
                log.error("Failed to send message to topic {}: {}", topic, ex.getMessage(), ex);
            } else {
                sendSuccessCounter.increment();
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
