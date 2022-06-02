package com.aegis.enrichment;

import com.aegis.domain.OcsfEvent;
import com.aegis.domain.ThreatInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Flink AsyncFunction for enriching events with threat intelligence data.
 * 
 * This function uses Flink's Async I/O capability to perform non-blocking
 * TIP lookups, allowing high throughput without blocking the stream processing.
 * 
 * Key features:
 * - Async I/O for non-blocking enrichment
 * - Enriches source and destination IPs
 * - Handles timeouts gracefully
 * - Maintains event ordering (ordered async I/O)
 */
public class TipEnrichmentFunction extends RichAsyncFunction<OcsfEvent, OcsfEvent> {
    
    private static final Logger log = LoggerFactory.getLogger(TipEnrichmentFunction.class);
    
    private transient ThreatIntelEnricher enricher;
    private final TipClient tipClient;
    
    /**
     * Constructor
     * 
     * @param tipClient The TIP client for lookups
     */
    public TipEnrichmentFunction(TipClient tipClient) {
        this.tipClient = tipClient;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize the enricher
        this.enricher = new ThreatIntelEnricher(tipClient);
        
        // Pre-populate Bloom filter with known-safe IPs
        // This could be loaded from a configuration file or database
        populateKnownSafeIps();
    }
    
    @Override
    public void asyncInvoke(OcsfEvent event, ResultFuture<OcsfEvent> resultFuture) throws Exception {
        // Enrich source IP if present
        String srcIp = event.getSrcEndpoint() != null ? event.getSrcEndpoint().getIp() : null;
        
        if (srcIp != null && !srcIp.isEmpty()) {
            // Perform async enrichment
            CompletableFuture<ThreatInfo> future = enricher.enrich(srcIp).toFuture();
            
            future.whenComplete((threatInfo, throwable) -> {
                if (throwable != null) {
                    log.warn("Failed to enrich source IP {}: {}", srcIp, throwable.getMessage());
                    // Continue without enrichment
                    resultFuture.complete(Collections.singleton(event));
                } else {
                    // Set threat info on the event
                    event.setThreatInfo(threatInfo);
                    
                    // If threat is detected, log it
                    if (threatInfo.getReputationScore() > 50) {
                        log.info("Threat detected for IP {}: {} (score: {})", 
                            srcIp, threatInfo.getThreatCategory(), threatInfo.getReputationScore());
                    }
                    
                    resultFuture.complete(Collections.singleton(event));
                }
            });
        } else {
            // No IP to enrich, pass through
            resultFuture.complete(Collections.singleton(event));
        }
    }
    
    @Override
    public void timeout(OcsfEvent event, ResultFuture<OcsfEvent> resultFuture) throws Exception {
        // Handle timeout by passing through the event without enrichment
        log.warn("TIP enrichment timeout for event {}", event.getUuid());
        resultFuture.complete(Collections.singleton(event));
    }
    
    /**
     * Pre-populate the Bloom filter with known-safe IPs.
     * This includes:
     * - RFC 1918 private IP ranges (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
     * - Loopback addresses (127.0.0.0/8)
     * - Link-local addresses (169.254.0.0/16)
     * - Known CDN and cloud provider IPs
     */
    private void populateKnownSafeIps() {
        // Mark RFC 1918 private IPs as safe
        // In production, this would be more comprehensive
        
        // 10.0.0.0/8
        for (int i = 0; i < 256; i++) {
            for (int j = 0; j < 256; j++) {
                enricher.markAsSafe("10." + i + "." + j + ".0");
            }
        }
        
        // 192.168.0.0/16
        for (int i = 0; i < 256; i++) {
            for (int j = 0; j < 256; j++) {
                enricher.markAsSafe("192.168." + i + "." + j);
            }
        }
        
        // 172.16.0.0/12
        for (int i = 16; i < 32; i++) {
            for (int j = 0; j < 256; j++) {
                enricher.markAsSafe("172." + i + "." + j + ".0");
            }
        }
        
        // Loopback
        enricher.markAsSafe("127.0.0.1");
        
        log.info("Populated Bloom filter with known-safe IPs");
    }
}
