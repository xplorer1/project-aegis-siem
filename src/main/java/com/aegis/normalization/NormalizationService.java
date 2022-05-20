package com.aegis.normalization;

import com.aegis.domain.OcsfEvent;
import com.aegis.domain.RawEvent;
import com.aegis.kafka.KafkaProducerService;
import com.aegis.normalization.parsers.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Normalization service that consumes raw events from Kafka,
 * parses them to OCSF format, and produces normalized events.
 */
@Service
public class NormalizationService {
    
    private static final Logger log = LoggerFactory.getLogger(NormalizationService.class);
    
    private final VendorDetector vendorDetector;
    private final ParserRegistry parserRegistry;
    private final KafkaProducerService producerService;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private final Map<String, Counter> parsedCounters = new HashMap<>();
    private final Map<String, Counter> failedCounters = new HashMap<>();
    
    public NormalizationService(
            VendorDetector vendorDetector,
            ParserRegistry parserRegistry,
            KafkaProducerService producerService,
            MeterRegistry meterRegistry) {
        this.vendorDetector = vendorDetector;
        this.parserRegistry = parserRegistry;
        this.producerService = producerService;
        this.meterRegistry = meterRegistry;
    }
    
    /**
     * Consume raw events from Kafka and normalize them
     */
    @KafkaListener(
        topics = "${aegis.kafka.topics.raw-events:raw-events}",
        groupId = "${aegis.kafka.consumer-group:normalization-service}",
        concurrency = "${aegis.normalization.concurrency:20}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void normalize(
            @Payload RawEvent rawEvent,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {
        
        try {
            log.debug("Processing raw event from partition {} offset {}", partition, offset);
            
            // 1. Detect vendor type
            String vendorType = vendorDetector.detectVendor(rawEvent.getData());
            log.debug("Detected vendor type: {}", vendorType);
            
            // 2. Get appropriate parser
            EventParser parser = parserRegistry.getParser(vendorType);
            
            // 3. Parse to OCSF
            OcsfEvent normalized = parser.parse(rawEvent.getData());
            
            // 4. Enrich with metadata
            enrichEvent(normalized, rawEvent, vendorType);
            
            // 5. Send to normalized-events topic
            producerService.sendNormalizedEvent(normalized);
            
            // 6. Update metrics
            incrementParsedCounter(vendorType);
            
            // 7. Acknowledge message
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            
            log.debug("Successfully normalized event of type {}", vendorType);
            
        } catch (ParseException e) {
            log.error("Failed to parse event: {}", e.getMessage());
            handleParseFailure(rawEvent, e);
            
            // Still acknowledge to avoid reprocessing
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            
        } catch (Exception e) {
            log.error("Unexpected error during normalization", e);
            handleParseFailure(rawEvent, e);
            
            // Acknowledge to avoid blocking
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }
    }
    
    /**
     * Enrich normalized event with additional metadata
     */
    private void enrichEvent(OcsfEvent event, RawEvent rawEvent, String vendorType) {
        // Set ingestion time
        event.setIngestionTime(Instant.now());
        
        // Set tenant ID from raw event
        if (rawEvent.getTenantId() != null) {
            event.setTenantId(rawEvent.getTenantId());
        }
        
        // Add vendor type to metadata if not already present
        if (event.getMetadata() == null) {
            event.setMetadata(new HashMap<>());
        }
        if (!event.getMetadata().containsKey("vendor")) {
            event.getMetadata().put("detectedVendor", vendorType);
        }
        
        // Add original ingestion timestamp
        if (rawEvent.getTimestamp() != null) {
            event.getMetadata().put("originalIngestionTime", rawEvent.getTimestamp().toString());
        }
    }
    
    /**
     * Handle parse failures by sending to dead letter queue
     */
    private void handleParseFailure(RawEvent rawEvent, Exception error) {
        try {
            // Create dead letter event
            Map<String, Object> deadLetter = new HashMap<>();
            deadLetter.put("rawData", new String(rawEvent.getData()));
            deadLetter.put("tenantId", rawEvent.getTenantId());
            deadLetter.put("timestamp", rawEvent.getTimestamp());
            deadLetter.put("error", error.getMessage());
            deadLetter.put("errorType", error.getClass().getSimpleName());
            deadLetter.put("failureTime", Instant.now());
            
            // Send to dead letter queue
            producerService.sendToDeadLetterQueue(deadLetter);
            
            // Update failure metrics
            String vendorType = vendorDetector.detectVendor(rawEvent.getData());
            incrementFailedCounter(vendorType);
            
            log.info("Sent failed event to dead letter queue");
            
        } catch (Exception e) {
            log.error("Failed to send event to dead letter queue", e);
        }
    }
    
    /**
     * Increment parsed event counter for vendor type
     */
    private void incrementParsedCounter(String vendorType) {
        Counter counter = parsedCounters.computeIfAbsent(vendorType, vt ->
            Counter.builder("aegis.normalization.parsed")
                .tag("vendor", vt)
                .description("Number of successfully parsed events by vendor")
                .register(meterRegistry)
        );
        counter.increment();
    }
    
    /**
     * Increment failed event counter for vendor type
     */
    private void incrementFailedCounter(String vendorType) {
        Counter counter = failedCounters.computeIfAbsent(vendorType, vt ->
            Counter.builder("aegis.normalization.failed")
                .tag("vendor", vt)
                .description("Number of failed parse attempts by vendor")
                .register(meterRegistry)
        );
        counter.increment();
    }
}
