package com.aegis.ingestion.buffer;

import com.aegis.domain.RawEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reflexive Buffer for handling Kafka backpressure.
 * When Kafka is unavailable or slow, events are spilled to local NVMe storage
 * using Chronicle Queue. When Kafka recovers, events are drained back.
 */
@Component
public class ReflexiveBuffer {
    
    private final ChronicleQueue diskQueue;
    private final AtomicBoolean spillMode = new AtomicBoolean(false);
    private final KafkaHealthCheck kafkaHealth;
    private final KafkaTemplate<String, RawEvent> kafkaTemplate;
    
    // Metrics
    private final Counter spillCounter;
    private final Counter drainCounter;
    
    @Autowired
    public ReflexiveBuffer(
            ChronicleQueue diskQueue,
            KafkaHealthCheck kafkaHealth,
            KafkaTemplate<String, RawEvent> kafkaTemplate,
            MeterRegistry meterRegistry) {
        this.diskQueue = diskQueue;
        this.kafkaHealth = kafkaHealth;
        this.kafkaTemplate = kafkaTemplate;
        
        // Initialize metrics
        this.spillCounter = Counter.builder("aegis.buffer.spill.events")
                .description("Number of events spilled to disk")
                .register(meterRegistry);
        this.drainCounter = Counter.builder("aegis.buffer.drain.events")
                .description("Number of events drained from disk")
                .register(meterRegistry);
    }
    
    /**
     * Spill an event to disk when Kafka is unavailable
     */
    public Mono<Void> spill(RawEvent event) {
        spillMode.set(true);
        return Mono.fromRunnable(() -> {
            try (ExcerptAppender appender = diskQueue.acquireAppender()) {
                byte[] serialized = event.serialize();
                appender.writeBytes(b -> {
                    b.writeInt(serialized.length);
                    b.write(serialized);
                });
                spillCounter.increment();
            }
        });
    }
    
    /**
     * Drain buffered events back to Kafka when it recovers
     */
    public void drainBuffer() {
        try (ExcerptTailer tailer = diskQueue.createTailer()) {
            while (tailer.readBytes(bytes -> {
                int length = bytes.readInt();
                byte[] data = new byte[length];
                bytes.read(data);
                RawEvent event = RawEvent.deserialize(data);
                
                // Send to Kafka synchronously during drain
                kafkaTemplate.send("raw-events", event).get();
                drainCounter.increment();
            })) {
                // Continue draining
            }
            spillMode.set(false);
        } catch (Exception e) {
            // Log error but don't fail - will retry on next scheduled check
            System.err.println("Error draining buffer: " + e.getMessage());
        }
    }
    
    /**
     * Scheduled check to drain buffer when Kafka is healthy
     */
    @Scheduled(fixedRate = 1000)
    public void checkKafkaHealth() {
        if (kafkaHealth.isHealthy() && spillMode.get()) {
            drainBuffer();
        }
    }
    
    public boolean isInSpillMode() {
        return spillMode.get();
    }
}
