package com.aegis.query;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;

/**
 * Metrics collector for query execution operations
 * Tracks query execution performance, errors, tier-specific metrics,
 * result sizes, and cache hit rates
 */
@Component
public class QueryMetrics {
    
    @Autowired
    private MeterRegistry meterRegistry;
    
    private Counter queriesExecuted;
    private Counter queriesFailed;
    private Counter queriesTimedOut;
    private Counter hotTierQueries;
    private Counter warmTierQueries;
    private Counter coldTierQueries;
    private Counter queryErrors;
    private Timer queryExecutionLatency;
    private Timer hotTierLatency;
    private Timer warmTierLatency;
    private Timer coldTierLatency;
    
    // New metrics for enhanced tracking
    private DistributionSummary resultSize;
    private DistributionSummary hotTierResultSize;
    private DistributionSummary warmTierResultSize;
    private DistributionSummary coldTierResultSize;
    private Counter cacheHits;
    private Counter cacheMisses;
    private Counter cacheEvictions;
    private Counter cacheInvalidations;
    
    @PostConstruct
    public void init() {
        queriesExecuted = Counter.builder("aegis.query.executed")
            .description("Total number of queries executed")
            .register(meterRegistry);
        
        queriesFailed = Counter.builder("aegis.query.failed")
            .description("Total number of queries that failed")
            .register(meterRegistry);
        
        queriesTimedOut = Counter.builder("aegis.query.timedout")
            .description("Total number of queries that timed out")
            .register(meterRegistry);
        
        hotTierQueries = Counter.builder("aegis.query.hot.tier")
            .description("Total number of hot tier queries executed")
            .register(meterRegistry);
        
        warmTierQueries = Counter.builder("aegis.query.warm.tier")
            .description("Total number of warm tier queries executed")
            .register(meterRegistry);
        
        coldTierQueries = Counter.builder("aegis.query.cold.tier")
            .description("Total number of cold tier queries executed")
            .register(meterRegistry);
        
        queryErrors = Counter.builder("aegis.query.errors")
            .description("Total number of query execution errors")
            .register(meterRegistry);
        
        // Timers with histogram support for percentile calculation
        queryExecutionLatency = Timer.builder("aegis.query.execution.latency")
            .description("Latency of overall query execution")
            .publishPercentiles(0.5, 0.95, 0.99) // P50, P95, P99
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofMillis(10))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .register(meterRegistry);
        
        hotTierLatency = Timer.builder("aegis.query.hot.tier.latency")
            .description("Latency of hot tier query execution")
            .publishPercentiles(0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofMillis(10))
            .maximumExpectedValue(Duration.ofSeconds(5))
            .register(meterRegistry);
        
        warmTierLatency = Timer.builder("aegis.query.warm.tier.latency")
            .description("Latency of warm tier query execution")
            .publishPercentiles(0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofMillis(10))
            .maximumExpectedValue(Duration.ofSeconds(10))
            .register(meterRegistry);
        
        coldTierLatency = Timer.builder("aegis.query.cold.tier.latency")
            .description("Latency of cold tier query execution")
            .publishPercentiles(0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofMillis(100))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .register(meterRegistry);
        
        // Result size distribution summaries
        resultSize = DistributionSummary.builder("aegis.query.result.size")
            .description("Distribution of query result sizes (number of records)")
            .baseUnit("records")
            .publishPercentiles(0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .minimumExpectedValue(1.0)
            .maximumExpectedValue(100000.0)
            .register(meterRegistry);
        
        hotTierResultSize = DistributionSummary.builder("aegis.query.hot.tier.result.size")
            .description("Distribution of hot tier query result sizes")
            .baseUnit("records")
            .publishPercentiles(0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(meterRegistry);
        
        warmTierResultSize = DistributionSummary.builder("aegis.query.warm.tier.result.size")
            .description("Distribution of warm tier query result sizes")
            .baseUnit("records")
            .publishPercentiles(0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(meterRegistry);
        
        coldTierResultSize = DistributionSummary.builder("aegis.query.cold.tier.result.size")
            .description("Distribution of cold tier query result sizes")
            .baseUnit("records")
            .publishPercentiles(0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(meterRegistry);
        
        // Cache metrics
        cacheHits = Counter.builder("aegis.query.cache.hits")
            .description("Total number of query cache hits")
            .register(meterRegistry);
        
        cacheMisses = Counter.builder("aegis.query.cache.misses")
            .description("Total number of query cache misses")
            .register(meterRegistry);
        
        cacheEvictions = Counter.builder("aegis.query.cache.evictions")
            .description("Total number of query cache evictions")
            .register(meterRegistry);
        
        cacheInvalidations = Counter.builder("aegis.query.cache.invalidations")
            .description("Total number of query cache invalidations")
            .register(meterRegistry);
    }
    
    public void recordQueryExecuted() {
        queriesExecuted.increment();
    }
    
    public void recordQueryFailed() {
        queriesFailed.increment();
    }
    
    public void recordQueryTimedOut() {
        queriesTimedOut.increment();
    }
    
    public void recordHotTierQuery() {
        hotTierQueries.increment();
    }
    
    public void recordWarmTierQuery() {
        warmTierQueries.increment();
    }
    
    public void recordColdTierQuery() {
        coldTierQueries.increment();
    }
    
    public void recordQueryError() {
        queryErrors.increment();
    }
    
    public Timer.Sample startQueryTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void recordQueryLatency(Timer.Sample sample) {
        sample.stop(queryExecutionLatency);
    }
    
    public Timer.Sample startHotTierTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void recordHotTierLatency(Timer.Sample sample) {
        sample.stop(hotTierLatency);
    }
    
    public Timer.Sample startWarmTierTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void recordWarmTierLatency(Timer.Sample sample) {
        sample.stop(warmTierLatency);
    }
    
    public Timer.Sample startColdTierTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void recordColdTierLatency(Timer.Sample sample) {
        sample.stop(coldTierLatency);
    }
    
    // Result size tracking methods
    public void recordResultSize(long size) {
        resultSize.record(size);
    }
    
    public void recordHotTierResultSize(long size) {
        hotTierResultSize.record(size);
    }
    
    public void recordWarmTierResultSize(long size) {
        warmTierResultSize.record(size);
    }
    
    public void recordColdTierResultSize(long size) {
        coldTierResultSize.record(size);
    }
    
    // Cache metrics methods
    public void recordCacheHit() {
        cacheHits.increment();
    }
    
    public void recordCacheMiss() {
        cacheMisses.increment();
    }
    
    public void recordCacheEviction() {
        cacheEvictions.increment();
    }
    
    public void recordCacheInvalidation() {
        cacheInvalidations.increment();
    }
    
    /**
     * Calculate cache hit rate as a percentage
     * @return cache hit rate (0-100) or 0 if no cache operations
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
    
    // Getter methods for testing
    public Counter getQueriesExecuted() {
        return queriesExecuted;
    }
    
    public Counter getQueriesFailed() {
        return queriesFailed;
    }
    
    public Counter getQueriesTimedOut() {
        return queriesTimedOut;
    }
    
    public Counter getHotTierQueries() {
        return hotTierQueries;
    }
    
    public Counter getWarmTierQueries() {
        return warmTierQueries;
    }
    
    public Counter getColdTierQueries() {
        return coldTierQueries;
    }
    
    public Counter getQueryErrors() {
        return queryErrors;
    }
    
    public Timer getQueryExecutionLatency() {
        return queryExecutionLatency;
    }
    
    public Timer getHotTierLatency() {
        return hotTierLatency;
    }
    
    public Timer getWarmTierLatency() {
        return warmTierLatency;
    }
    
    public Timer getColdTierLatency() {
        return coldTierLatency;
    }
    
    public DistributionSummary getResultSize() {
        return resultSize;
    }
    
    public DistributionSummary getHotTierResultSize() {
        return hotTierResultSize;
    }
    
    public DistributionSummary getWarmTierResultSize() {
        return warmTierResultSize;
    }
    
    public DistributionSummary getColdTierResultSize() {
        return coldTierResultSize;
    }
    
    public Counter getCacheHits() {
        return cacheHits;
    }
    
    public Counter getCacheMisses() {
        return cacheMisses;
    }
    
    public Counter getCacheEvictions() {
        return cacheEvictions;
    }
    
    public Counter getCacheInvalidations() {
        return cacheInvalidations;
    }
}
