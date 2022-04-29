package com.aegis.ingestion.hec;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * HTTP Event Collector (HEC) Endpoint
 * Splunk HEC-compatible REST API for event ingestion
 * Supports both single events and batch ingestion
 */
@RestController
@RequestMapping("/services/collector")
public class HecEndpoint {
    private static final Logger log = LoggerFactory.getLogger(HecEndpoint.class);
    
    private final MeterRegistry meterRegistry;
    private Counter eventsReceived;
    private Counter eventsProcessed;
    private Counter eventsFailed;
    private Counter requestsReceived;
    private Counter requestsRejected;
    private Timer processingTimer;
    
    public HecEndpoint(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        initializeMetrics();
    }
    
    private void initializeMetrics() {
        this.eventsReceived = Counter.builder("ingestion.hec.events.received")
            .description("Total HEC events received")
            .register(meterRegistry);
        
        this.eventsProcessed = Counter.builder("ingestion.hec.events.processed")
            .description("Total HEC events successfully processed")
            .register(meterRegistry);
        
        this.eventsFailed = Counter.builder("ingestion.hec.events.failed")
            .description("Total HEC events that failed processing")
            .register(meterRegistry);
        
        this.requestsReceived = Counter.builder("ingestion.hec.requests.received")
            .description("Total HEC requests received")
            .register(meterRegistry);
        
        this.requestsRejected = Counter.builder("ingestion.hec.requests.rejected")
            .description("Total HEC requests rejected")
            .register(meterRegistry);
        
        this.processingTimer = Timer.builder("ingestion.hec.processing.time")
            .description("Time to process HEC requests")
            .register(meterRegistry);
    }
    
    /**
     * HEC event ingestion endpoint
     * POST /services/collector/event
     * @param token Authorization token (format: "Splunk <token>")
     * @param events JSON event or array of events
     * @return HecResponse with ingestion status
     */
    @PostMapping(value = "/event", consumes = MediaType.APPLICATION_JSON_VALUE, 
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<HecResponse> ingestEvent(
            @RequestHeader("Authorization") String token,
            @RequestBody Flux<JsonNode> events) {
        
        requestsReceived.increment();
        log.debug("Received HEC event ingestion request");
        
        // Validate and extract token
        String extractedToken = extractToken(token);
        if (extractedToken == null) {
            log.warn("Invalid HEC authorization token");
            requestsRejected.increment();
            return Mono.just(new HecResponse(0, "Error: Invalid authorization token"));
        }
        
        // TODO: Validate token against tenant database
        // For now, accept any non-empty token
        String tenantId = extractTenantFromToken(extractedToken);
        
        // TODO: Apply rate limiting (will be implemented in later tasks)
        
        return processingTimer.record(() -> {
            return events
                .doOnNext(event -> eventsReceived.increment())
                .flatMap(event -> processEvent(event, tenantId))
                .count()
                .map(count -> {
                    log.debug("Processed {} HEC events for tenant {}", count, tenantId);
                    return new HecResponse(count, "Success");
                })
                .onErrorResume(error -> {
                    log.error("Failed to process HEC events", error);
                    eventsFailed.increment();
                    return Mono.just(new HecResponse(0, "Error: " + error.getMessage()));
                });
        });
    }
    
    /**
     * Extract token from Authorization header
     * Expected format: "Splunk <token>" or just "<token>"
     * @param authHeader Authorization header value
     * @return Extracted token or null if invalid
     */
    private String extractToken(String authHeader) {
        if (authHeader == null || authHeader.trim().isEmpty()) {
            return null;
        }
        
        String token = authHeader.trim();
        if (token.startsWith("Splunk ")) {
            token = token.substring(7).trim();
        }
        
        return token.isEmpty() ? null : token;
    }
    
    /**
     * Extract tenant ID from token
     * In production, this would lookup the tenant from a database
     * @param token HEC token
     * @return Tenant ID
     */
    private String extractTenantFromToken(String token) {
        // For now, use a simple hash of the token as tenant ID
        // In production, this would be a database lookup
        return "tenant-" + Math.abs(token.hashCode() % 1000);
    }
    
    /**
     * HEC health check endpoint
     * GET /services/collector/health
     * @return Health status
     */
    @GetMapping(value = "/health", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<HecHealthResponse> health() {
        return Mono.just(new HecHealthResponse("healthy"));
    }
    
    /**
     * Process a single HEC event
     * @param event JSON event node
     * @param tenantId Tenant identifier
     * @return Mono that completes when event is processed
     */
    private Mono<Void> processEvent(JsonNode event, String tenantId) {
        return Mono.defer(() -> {
            try {
                // Validate event structure
                if (event == null || event.isEmpty()) {
                    log.warn("Received empty HEC event");
                    eventsFailed.increment();
                    return Mono.empty();
                }
                
                // Validate required HEC fields
                if (!event.has("event")) {
                    log.warn("HEC event missing required 'event' field");
                    eventsFailed.increment();
                    return Mono.empty();
                }
                
                // Extract event data
                JsonNode eventData = event.get("event");
                Long time = event.has("time") ? event.get("time").asLong() : null;
                String host = event.has("host") ? event.get("host").asText() : null;
                String source = event.has("source") ? event.get("source").asText() : null;
                String sourcetype = event.has("sourcetype") ? event.get("sourcetype").asText() : null;
                String index = event.has("index") ? event.get("index").asText() : null;
                
                // Log event metadata at trace level
                if (log.isTraceEnabled()) {
                    log.trace("Processing HEC event - tenant: {}, time: {}, host: {}, source: {}, sourcetype: {}, index: {}", 
                        tenantId, time, host, source, sourcetype, index);
                }
                
                // TODO: Create RawEvent and send to Kafka producer (will be implemented in later tasks)
                // For now, just count as processed
                eventsProcessed.increment();
                
                return Mono.empty();
                
            } catch (Exception e) {
                log.error("Failed to process HEC event", e);
                eventsFailed.increment();
                return Mono.error(e);
            }
        });
    }
}
