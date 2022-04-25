package com.aegis.ingestion.syslog;

import com.aegis.domain.RawEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;

import java.time.Instant;

/**
 * Reactive Syslog UDP Listener
 * Handles high-throughput UDP syslog event ingestion using Netty
 * Supports burst traffic and backpressure handling
 */
@Component
public class SyslogUdpListener {
    private static final Logger log = LoggerFactory.getLogger(SyslogUdpListener.class);
    
    private final MeterRegistry meterRegistry;
    private Counter eventsReceived;
    private Counter eventsProcessed;
    private Counter eventsFailed;
    
    public SyslogUdpListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
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
     * @param rawBytes The raw event bytes
     * @return Mono that completes when event is handled
     */
    private Mono<Void> handleEvent(byte[] rawBytes) {
        eventsReceived.increment();
        
        return Mono.fromCallable(() -> {
            try {
                // Create RawEvent with current timestamp
                RawEvent event = new RawEvent(rawBytes, Instant.now());
                
                // TODO: Send to Kafka producer (will be implemented in later tasks)
                // For now, just log and count
                log.debug("Received UDP event: {} bytes", rawBytes.length);
                eventsProcessed.increment();
                
                return event;
            } catch (Exception e) {
                log.error("Failed to process UDP event", e);
                eventsFailed.increment();
                throw e;
            }
        }).then();
    }
}
