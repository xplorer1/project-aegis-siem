package com.aegis.ingestion.syslog;

import com.aegis.domain.RawEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Reactive Syslog UDP Listener
 * Handles high-throughput UDP syslog event ingestion using Netty
 * Supports burst traffic and backpressure handling
 */
@Component
public class SyslogUdpListener {
    private static final Logger log = LoggerFactory.getLogger(SyslogUdpListener.class);
    private static final int MAX_EVENT_SIZE = 65507; // Max UDP packet size
    
    private final MeterRegistry meterRegistry;
    private Counter eventsReceived;
    private Counter eventsProcessed;
    private Counter eventsFailed;
    private Counter eventsOversized;
    private Counter eventsDropped;
    private Timer processingTimer;
    
    // Backpressure handling
    private final java.util.concurrent.Semaphore backpressureSemaphore;
    private static final int MAX_CONCURRENT_EVENTS = 10000; // Limit concurrent processing
    
    public SyslogUdpListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.backpressureSemaphore = new java.util.concurrent.Semaphore(MAX_CONCURRENT_EVENTS);
        initializeMetrics();
    }
    
    private void initializeMetrics() {
        this.eventsReceived = Counter.builder("ingestion.udp.events.received")
            .description("Total UDP events received")
            .register(meterRegistry);
        
        this.eventsProcessed = Counter.builder("ingestion.udp.events.processed")
            .description("Total UDP events successfully processed")
            .register(meterRegistry);
        
        this.eventsFailed = Counter.builder("ingestion.udp.events.failed")
            .description("Total UDP events that failed processing")
            .register(meterRegistry);
        
        this.eventsOversized = Counter.builder("ingestion.udp.events.oversized")
            .description("Total UDP events rejected due to size")
            .register(meterRegistry);
        
        this.eventsDropped = Counter.builder("ingestion.udp.events.dropped")
            .description("Total UDP events dropped due to backpressure")
            .register(meterRegistry);
        
        this.processingTimer = Timer.builder("ingestion.udp.processing.time")
            .description("Time to process UDP events")
            .register(meterRegistry);
        
        // Register gauge for available permits
        meterRegistry.gauge("ingestion.udp.backpressure.available", 
            backpressureSemaphore, java.util.concurrent.Semaphore::availablePermits);
    }
    
    /**
     * Start listening for UDP syslog events on the specified port
     * @param port The UDP port to bind to
     * @return Mono that completes when the server is disposed
     */
    public Mono<Void> listen(int port) {
        return listen(port, 65536); // Default 64KB buffer
    }
    
    /**
     * Start listening for UDP syslog events on the specified port with custom buffer size
     * @param port The UDP port to bind to
     * @param bufferSize The receive buffer size in bytes
     * @return Mono that completes when the server is disposed
     */
    public Mono<Void> listen(int port, int bufferSize) {
        log.info("Starting UDP listener on port {} with buffer size {} bytes", port, bufferSize);
        
        return UdpServer.create()
            .port(port)
            .option(io.netty.channel.ChannelOption.SO_RCVBUF, bufferSize)
            .option(io.netty.channel.ChannelOption.SO_REUSEADDR, true)
            .wiretap(false) // Disable wiretap in production for performance
            .handle((in, out) -> {
                return in.receive()
                    .asByteArray()
                    .flatMap(bytes -> handleEvent(bytes))
                    .then();
            })
            .bind()
            .doOnSuccess(connection -> {
                log.info("UDP listener bound successfully on port {} with buffer size {} bytes", 
                    port, bufferSize);
            })
            .doOnError(error -> {
                log.error("Failed to bind UDP listener on port {}", port, error);
            })
            .flatMap(Connection::onDispose);
    }
    
    /**
     * Handle incoming UDP event
     * Validates event size, creates RawEvent, and prepares for Kafka ingestion
     * Implements backpressure handling to prevent memory exhaustion
     * @param rawBytes The raw event bytes
     * @return Mono that completes when event is handled
     */
    private Mono<Void> handleEvent(byte[] rawBytes) {
        return Mono.defer(() -> {
            eventsReceived.increment();
            
            // Try to acquire permit for backpressure control
            if (!backpressureSemaphore.tryAcquire()) {
                log.warn("Backpressure limit reached, dropping UDP event");
                eventsDropped.increment();
                return Mono.empty();
            }
            
            return processingTimer.record(() -> {
                try {
                    // Validate event size
                    if (rawBytes == null || rawBytes.length == 0) {
                        log.warn("Received empty UDP event");
                        eventsFailed.increment();
                        return Mono.empty();
                    }
                    
                    if (rawBytes.length > MAX_EVENT_SIZE) {
                        log.warn("Received oversized UDP event: {} bytes (max: {})", 
                            rawBytes.length, MAX_EVENT_SIZE);
                        eventsOversized.increment();
                        return Mono.empty();
                    }
                    
                    // Create RawEvent with current timestamp
                    RawEvent event = new RawEvent(rawBytes, Instant.now());
                    
                    // Log event details at trace level
                    if (log.isTraceEnabled()) {
                        String preview = new String(rawBytes, 0, 
                            Math.min(100, rawBytes.length), StandardCharsets.UTF_8);
                        log.trace("Received UDP event: {} bytes, preview: {}", 
                            rawBytes.length, preview);
                    }
                    
                    // TODO: Send to Kafka producer (will be implemented in later tasks)
                    // For now, just count as processed
                    eventsProcessed.increment();
                    
                    return Mono.empty();
                    
                } catch (Exception e) {
                    log.error("Failed to process UDP event", e);
                    eventsFailed.increment();
                    return Mono.error(e);
                } finally {
                    // Release permit
                    backpressureSemaphore.release();
                }
            });
        }).then();
    }
}
