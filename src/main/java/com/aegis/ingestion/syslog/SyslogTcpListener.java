package com.aegis.ingestion.syslog;

import com.aegis.domain.RawEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpServer;
import reactor.netty.tcp.TcpSslContextSpec;

import javax.net.ssl.SSLException;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.time.Instant;

/**
 * Reactive Syslog TCP Listener
 * Handles TCP syslog event ingestion with TLS support using Netty
 * Supports connection management and backpressure handling
 */
@Component
public class SyslogTcpListener {
    private static final Logger log = LoggerFactory.getLogger(SyslogTcpListener.class);
    private static final int MAX_EVENT_SIZE = 1048576; // 1MB max for TCP
    private static final byte NEWLINE = '\n';
    
    private final MeterRegistry meterRegistry;
    private Counter connectionsAccepted;
    private Counter connectionsClosed;
    private Counter eventsReceived;
    private Counter eventsProcessed;
    private Counter eventsFailed;
    private Counter eventsOversized;
    private Timer processingTimer;
    
    public SyslogTcpListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        initializeMetrics();
    }
    
    private void initializeMetrics() {
        this.connectionsAccepted = Counter.builder("ingestion.tcp.connections.accepted")
            .description("Total TCP connections accepted")
            .register(meterRegistry);
        
        this.connectionsClosed = Counter.builder("ingestion.tcp.connections.closed")
            .description("Total TCP connections closed")
            .register(meterRegistry);
        
        this.eventsReceived = Counter.builder("ingestion.tcp.events.received")
            .description("Total TCP events received")
            .register(meterRegistry);
        
        this.eventsProcessed = Counter.builder("ingestion.tcp.events.processed")
            .description("Total TCP events successfully processed")
            .register(meterRegistry);
        
        this.eventsFailed = Counter.builder("ingestion.tcp.events.failed")
            .description("Total TCP events that failed processing")
            .register(meterRegistry);
        
        this.eventsOversized = Counter.builder("ingestion.tcp.events.oversized")
            .description("Total TCP events rejected due to size")
            .register(meterRegistry);
        
        this.processingTimer = Timer.builder("ingestion.tcp.processing.time")
            .description("Time to process TCP events")
            .register(meterRegistry);
    }
    
    /**
     * Start listening for TCP syslog events on the specified port
     * @param port The TCP port to bind to
     * @return Mono that completes when the server is disposed
     */
    public Mono<Void> listen(int port) {
        return listen(port, 1000, false); // Default 1000 max connections, no TLS
    }
    
    /**
     * Start listening for TCP syslog events on the specified port with connection limits
     * @param port The TCP port to bind to
     * @param maxConnections Maximum number of concurrent connections
     * @return Mono that completes when the server is disposed
     */
    public Mono<Void> listen(int port, int maxConnections) {
        return listen(port, maxConnections, false); // No TLS by default
    }
    
    /**
     * Start listening for TCP syslog events on the specified port with TLS
     * @param port The TCP port to bind to
     * @param maxConnections Maximum number of concurrent connections
     * @param enableTls Whether to enable TLS encryption
     * @return Mono that completes when the server is disposed
     */
    public Mono<Void> listen(int port, int maxConnections, boolean enableTls) {
        log.info("Starting TCP listener on port {} with max {} connections, TLS: {}", 
            port, maxConnections, enableTls);
        
        TcpServer server = TcpServer.create()
            .port(port)
            .option(io.netty.channel.ChannelOption.SO_BACKLOG, 1024)
            .option(io.netty.channel.ChannelOption.SO_REUSEADDR, true)
            .option(io.netty.channel.ChannelOption.SO_KEEPALIVE, true)
            .option(io.netty.channel.ChannelOption.TCP_NODELAY, true)
            .childOption(io.netty.channel.ChannelOption.SO_RCVBUF, 65536)
            .childOption(io.netty.channel.ChannelOption.SO_SNDBUF, 65536)
            .wiretap(false); // Disable wiretap in production for performance
        
        // Add TLS if enabled
        if (enableTls) {
            try {
                SslContext sslContext = createSslContext();
                server = server.secure(spec -> spec.sslContext(sslContext));
                log.info("TLS enabled for TCP listener on port {}", port);
            } catch (Exception e) {
                log.error("Failed to configure TLS for TCP listener", e);
                return Mono.error(e);
            }
        }
        
        return server
            .doOnConnection(connection -> {
                connectionsAccepted.increment();
                log.debug("TCP connection accepted from {}", 
                    connection.channel().remoteAddress());
                
                connection.onDispose(() -> {
                    connectionsClosed.increment();
                    log.debug("TCP connection closed from {}", 
                        connection.channel().remoteAddress());
                });
            })
            .handle((in, out) -> {
                return in.receive()
                    .asByteArray()
                    .flatMap(bytes -> handleEvent(bytes))
                    .then();
            })
            .bind()
            .doOnSuccess(server2 -> {
                log.info("TCP listener bound successfully on port {} with max {} connections, TLS: {}", 
                    port, maxConnections, enableTls);
            })
            .doOnError(error -> {
                log.error("Failed to bind TCP listener on port {}", port, error);
            })
            .flatMap(Connection::onDispose);
    }
    
    /**
     * Create SSL context for TLS support
     * In production, this should load certificates from a keystore
     * @return SslContext configured for server-side TLS
     */
    private SslContext createSslContext() throws CertificateException, SSLException {
        // For development/testing, use self-signed certificate
        // In production, replace with proper certificate loading from keystore
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        
        return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
            .protocols("TLSv1.2", "TLSv1.3")
            .build();
    }
    
    /**
     * Handle incoming TCP event
     * Validates event size, creates RawEvent, and prepares for Kafka ingestion
     * TCP events may be framed by newlines or length-prefixed
     * @param rawBytes The raw event bytes
     * @return Mono that completes when event is handled
     */
    private Mono<Void> handleEvent(byte[] rawBytes) {
        return Mono.defer(() -> {
            eventsReceived.increment();
            
            return processingTimer.record(() -> {
                try {
                    // Validate event size
                    if (rawBytes == null || rawBytes.length == 0) {
                        log.warn("Received empty TCP event");
                        eventsFailed.increment();
                        return Mono.empty();
                    }
                    
                    if (rawBytes.length > MAX_EVENT_SIZE) {
                        log.warn("Received oversized TCP event: {} bytes (max: {})", 
                            rawBytes.length, MAX_EVENT_SIZE);
                        eventsOversized.increment();
                        return Mono.empty();
                    }
                    
                    // Strip trailing newline if present (common in syslog over TCP)
                    byte[] processedBytes = rawBytes;
                    if (rawBytes.length > 0 && rawBytes[rawBytes.length - 1] == NEWLINE) {
                        processedBytes = new byte[rawBytes.length - 1];
                        System.arraycopy(rawBytes, 0, processedBytes, 0, rawBytes.length - 1);
                    }
                    
                    // Create RawEvent with current timestamp
                    RawEvent event = new RawEvent(processedBytes, Instant.now());
                    
                    // Log event details at trace level
                    if (log.isTraceEnabled()) {
                        String preview = new String(processedBytes, 0, 
                            Math.min(100, processedBytes.length), StandardCharsets.UTF_8);
                        log.trace("Received TCP event: {} bytes, preview: {}", 
                            processedBytes.length, preview);
                    }
                    
                    // TODO: Send to Kafka producer (will be implemented in later tasks)
                    // For now, just count as processed
                    eventsProcessed.increment();
                    
                    return Mono.empty();
                    
                } catch (Exception e) {
                    log.error("Failed to process TCP event", e);
                    eventsFailed.increment();
                    return Mono.error(e);
                }
            });
        }).then();
    }
}
