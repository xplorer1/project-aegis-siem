package com.aegis.enrichment;

import com.aegis.domain.ThreatInfo;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * ThreatIntelEnricher provides threat intelligence enrichment for security events.
 * It uses a multi-tier caching strategy:
 * 1. Bloom filter for known-safe IPs (fast negative lookup)
 * 2. Caffeine cache for recent lookups (1M entries, 10min TTL)
 * 3. External TIP API for cache misses
 * 
 * This design minimizes network calls and achieves sub-millisecond enrichment
 * for 99% of traffic (known-safe IPs).
 */
@Component
public class ThreatIntelEnricher {
    
    private final LoadingCache<String, ThreatInfo> cache;
    private final BloomFilter<String> knownSafe;
    private final TipClient tipClient;
    private final EnrichmentMetrics metrics;
    
    /**
     * Constructor for ThreatIntelEnricher
     * 
     * @param tipClient The threat intelligence platform client
     * @param metrics The metrics collector
     */
    @Autowired
    public ThreatIntelEnricher(TipClient tipClient, EnrichmentMetrics metrics) {
        this.tipClient = tipClient;
        this.metrics = metrics;
        
        // Configure Caffeine cache with 1M entries and 10min TTL
        // This provides fast lookups for recently queried IPs/domains
        this.cache = Caffeine.newBuilder()
            .maximumSize(1_000_000)  // Maximum 1 million entries
            .expireAfterWrite(10, TimeUnit.MINUTES)  // 10 minute TTL
            .recordStats()  // Enable statistics for monitoring
            .build(key -> {
                // Loader function: fetch from TIP on cache miss
                return tipClient.lookup(key).block();
            });
        
        // Initialize Bloom filter for known-safe IPs
        // Expected insertions: 50 million (covers typical enterprise internal IPs + known CDNs)
        // False positive rate: 1% (acceptable trade-off for performance)
        this.knownSafe = BloomFilter.create(
            Funnels.stringFunnel(StandardCharsets.UTF_8),
            50_000_000,  // 50M expected insertions
            0.01         // 1% false positive rate
        );
    }
    
    /**
     * Enrich an IP address with threat intelligence data.
     * 
     * Fast path: Check Bloom filter for known-safe IPs
     * Slow path: Check cache, then external TIP API
     * 
     * @param ip The IP address to enrich
     * @return Mono containing ThreatInfo
     */
    public Mono<ThreatInfo> enrich(String ip) {
        long startTime = System.currentTimeMillis();
        
        // Fast path: Check Bloom filter first
        // If IP is in the known-safe set, return immediately without cache/API lookup
        if (knownSafe.mightContain(ip)) {
            metrics.recordBloomFilterHit();
            metrics.recordEnrichmentSuccess();
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordEnrichmentLatency(duration);
            return Mono.just(ThreatInfo.SAFE);
        }
        
        metrics.recordBloomFilterMiss();
        
        // Slow path: Check cache (which will call TIP API on miss)
        return Mono.fromCallable(() -> {
            long lookupStart = System.currentTimeMillis();
            
            try {
                ThreatInfo result = cache.get(ip);
                
                // Check if this was a cache hit or miss
                // Note: Caffeine doesn't provide a direct way to check this,
                // so we track it in the TipClient
                
                metrics.recordEnrichmentSuccess();
                long duration = System.currentTimeMillis() - startTime;
                metrics.recordEnrichmentLatency(duration);
                
                long lookupDuration = System.currentTimeMillis() - lookupStart;
                metrics.recordLookupLatency(lookupDuration);
                
                return result;
            } catch (Exception e) {
                metrics.recordEnrichmentFailure();
                throw e;
            }
        }).onErrorResume(e -> {
            // If cache lookup fails, return safe to avoid blocking pipeline
            metrics.recordEnrichmentFailure();
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordEnrichmentLatency(duration);
            return Mono.just(ThreatInfo.SAFE);
        });
    }
    
    /**
     * Enrich a domain with threat intelligence data.
     * 
     * @param domain The domain to enrich
     * @return Mono containing ThreatInfo
     */
    public Mono<ThreatInfo> enrichDomain(String domain) {
        long startTime = System.currentTimeMillis();
        
        // Domains don't use Bloom filter (too many possible values)
        // Go straight to cache
        return Mono.fromCallable(() -> {
            try {
                ThreatInfo result = cache.get(domain);
                metrics.recordEnrichmentSuccess();
                long duration = System.currentTimeMillis() - startTime;
                metrics.recordEnrichmentLatency(duration);
                return result;
            } catch (Exception e) {
                metrics.recordEnrichmentFailure();
                throw e;
            }
        }).onErrorResume(e -> {
            metrics.recordEnrichmentFailure();
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordEnrichmentLatency(duration);
            return Mono.just(ThreatInfo.SAFE);
        });
    }
    
    /**
     * Add an IP to the known-safe Bloom filter.
     * This is used for whitelisting internal IPs and known-good infrastructure.
     * 
     * @param ip The IP address to mark as safe
     */
    public void markAsSafe(String ip) {
        knownSafe.put(ip);
    }
    
    /**
     * Check if an IP might be in the known-safe set.
     * Note: Bloom filters can have false positives but never false negatives.
     * 
     * @param ip The IP address to check
     * @return true if the IP might be safe, false if definitely not safe
     */
    public boolean mightBeSafe(String ip) {
        return knownSafe.mightContain(ip);
    }
    
    /**
     * Get cache statistics for monitoring.
     * Returns metrics like hit rate, miss rate, eviction count.
     * 
     * @return Cache statistics
     */
    public com.github.benmanes.caffeine.cache.stats.CacheStats getCacheStats() {
        return cache.stats();
    }
    
    /**
     * Get the current cache size.
     * 
     * @return Number of entries in the cache
     */
    public long getCacheSize() {
        return cache.estimatedSize();
    }
}
