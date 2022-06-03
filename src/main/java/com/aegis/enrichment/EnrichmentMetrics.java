package com.aegis.enrichment;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Metrics collector for threat intelligence enrichment operations.
 * 
 * Tracks:
 * - Cache hit rate (hits vs misses)
 * - Bloom filter hit rate
 * - TIP API lookup latency
 * - Enrichment success/failure rates
 * - Total enrichment throughput
 * 
 * These metrics are exposed via Micrometer and can be scraped by Prometheus.
 */
@Component
public class EnrichmentMetrics {
    
    private final Counter cacheHits;
    private final Counter cacheMisses;
    private final Counter bloomFilterHits;
    private final Counter bloomFilterMisses;
    private final Counter tipApiCalls;
    private final Counter tipApiErrors;
    private final Counter enrichmentSuccess;
    private final Counter enrichmentFailures;
    private final Timer lookupLatency;
    private final Timer enrichmentLatency;
    
    /**
     * Constructor initializes all metrics with the MeterRegistry.
     * 
     * @param registry The Micrometer MeterRegistry
     */
    public EnrichmentMetrics(MeterRegistry registry) {
        // Cache metrics
        this.cacheHits = Counter.builder("aegis.enrichment.cache.hits")
            .description("Number of cache hits for TIP lookups")
            .tag("component", "enrichment")
            .register(registry);
        
        this.cacheMisses = Counter.builder("aegis.enrichment.cache.misses")
            .description("Number of cache misses for TIP lookups")
            .tag("component", "enrichment")
            .register(registry);
        
        // Bloom filter metrics
        this.bloomFilterHits = Counter.builder("aegis.enrichment.bloom.hits")
            .description("Number of Bloom filter hits (known-safe IPs)")
            .tag("component", "enrichment")
            .register(registry);
        
        this.bloomFilterMisses = Counter.builder("aegis.enrichment.bloom.misses")
            .description("Number of Bloom filter misses (unknown IPs)")
            .tag("component", "enrichment")
            .register(registry);
        
        // TIP API metrics
        this.tipApiCalls = Counter.builder("aegis.enrichment.tip.calls")
            .description("Number of TIP API calls made")
            .tag("component", "enrichment")
            .register(registry);
        
        this.tipApiErrors = Counter.builder("aegis.enrichment.tip.errors")
            .description("Number of TIP API errors")
            .tag("component", "enrichment")
            .register(registry);
        
        // Enrichment outcome metrics
        this.enrichmentSuccess = Counter.builder("aegis.enrichment.success")
            .description("Number of successful enrichments")
            .tag("component", "enrichment")
            .register(registry);
        
        this.enrichmentFailures = Counter.builder("aegis.enrichment.failures")
            .description("Number of failed enrichments")
            .tag("component", "enrichment")
            .register(registry);
        
        // Latency metrics
        this.lookupLatency = Timer.builder("aegis.enrichment.lookup.latency")
            .description("Latency of TIP lookups (including cache)")
            .tag("component", "enrichment")
            .publishPercentiles(0.5, 0.95, 0.99)  // P50, P95, P99
            .register(registry);
        
        this.enrichmentLatency = Timer.builder("aegis.enrichment.latency")
            .description("End-to-end enrichment latency")
            .tag("component", "enrichment")
            .publishPercentiles(0.5, 0.95, 0.99)  // P50, P95, P99
            .register(registry);
    }
    
    /**
     * Record a cache hit.
     */
    public void recordCacheHit() {
        cacheHits.increment();
    }
    
    /**
     * Record a cache miss.
     */
    public void recordCacheMiss() {
        cacheMisses.increment();
    }
    
    /**
     * Record a Bloom filter hit (known-safe IP).
     */
    public void recordBloomFilterHit() {
        bloomFilterHits.increment();
    }
    
    /**
     * Record a Bloom filter miss (unknown IP).
     */
    public void recordBloomFilterMiss() {
        bloomFilterMisses.increment();
    }
    
    /**
     * Record a TIP API call.
     */
    public void recordTipApiCall() {
        tipApiCalls.increment();
    }
    
    /**
     * Record a TIP API error.
     */
    public void recordTipApiError() {
        tipApiErrors.increment();
    }
    
    /**
     * Record a successful enrichment.
     */
    public void recordEnrichmentSuccess() {
        enrichmentSuccess.increment();
    }
    
    /**
     * Record a failed enrichment.
     */
    public void recordEnrichmentFailure() {
        enrichmentFailures.increment();
    }
    
    /**
     * Record lookup latency.
     * 
     * @param durationMs Duration in milliseconds
     */
    public void recordLookupLatency(long durationMs) {
        lookupLatency.record(durationMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Record enrichment latency.
     * 
     * @param durationMs Duration in milliseconds
     */
    public void recordEnrichmentLatency(long durationMs) {
        enrichmentLatency.record(durationMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Get a Timer.Sample for measuring latency.
     * Use with recordSample() to record the elapsed time.
     * 
     * @param registry The MeterRegistry
     * @return Timer.Sample
     */
    public static Timer.Sample startTimer(MeterRegistry registry) {
        return Timer.start(registry);
    }
    
    /**
     * Record a timer sample for lookup latency.
     * 
     * @param sample The timer sample to record
     */
    public void recordLookupSample(Timer.Sample sample) {
        sample.stop(lookupLatency);
    }
    
    /**
     * Record a timer sample for enrichment latency.
     * 
     * @param sample The timer sample to record
     */
    public void recordEnrichmentSample(Timer.Sample sample) {
        sample.stop(enrichmentLatency);
    }
    
    /**
     * Calculate and return the cache hit rate.
     * 
     * @return Cache hit rate as a percentage (0-100)
     */
    public double getCacheHitRate() {
        double hits = cacheHits.count();
        double misses = cacheMisses.count();
        double total = hits + misses;
        
        if (total == 0) {
            return 0.0;
        }
        
        return (hits / total) * 100.0;
    }
    
    /**
     * Calculate and return the Bloom filter hit rate.
     * 
     * @return Bloom filter hit rate as a percentage (0-100)
     */
    public double getBloomFilterHitRate() {
        double hits = bloomFilterHits.count();
        double misses = bloomFilterMisses.count();
        double total = hits + misses;
        
        if (total == 0) {
            return 0.0;
        }
        
        return (hits / total) * 100.0;
    }
}
