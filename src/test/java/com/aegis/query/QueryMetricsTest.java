package com.aegis.query;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Comprehensive test suite for QueryMetrics
 * Tests all metric tracking functionality including:
 * - Query execution counters
 * - Execution time histograms with percentiles
 * - Result size tracking
 * - Cache hit rate calculations
 */
@DisplayName("QueryMetrics Tests")
class QueryMetricsTest {
    
    private QueryMetrics queryMetrics;
    private MeterRegistry meterRegistry;
    
    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        queryMetrics = new QueryMetrics();
        queryMetrics.meterRegistry = meterRegistry;
        queryMetrics.init();
    }
    
    @Test
    @DisplayName("Should initialize all metrics on startup")
    void shouldInitializeAllMetrics() {
        assertThat(queryMetrics.getQueriesExecuted()).isNotNull();
        assertThat(queryMetrics.getQueriesFailed()).isNotNull();
        assertThat(queryMetrics.getQueriesTimedOut()).isNotNull();
        assertThat(queryMetrics.getHotTierQueries()).isNotNull();
        assertThat(queryMetrics.getWarmTierQueries()).isNotNull();
        assertThat(queryMetrics.getColdTierQueries()).isNotNull();
        assertThat(queryMetrics.getQueryErrors()).isNotNull();
        assertThat(queryMetrics.getQueryExecutionLatency()).isNotNull();
        assertThat(queryMetrics.getHotTierLatency()).isNotNull();
        assertThat(queryMetrics.getWarmTierLatency()).isNotNull();
        assertThat(queryMetrics.getColdTierLatency()).isNotNull();
        assertThat(queryMetrics.getResultSize()).isNotNull();
        assertThat(queryMetrics.getHotTierResultSize()).isNotNull();
        assertThat(queryMetrics.getWarmTierResultSize()).isNotNull();
        assertThat(queryMetrics.getColdTierResultSize()).isNotNull();
        assertThat(queryMetrics.getCacheHits()).isNotNull();
        assertThat(queryMetrics.getCacheMisses()).isNotNull();
        assertThat(queryMetrics.getCacheEvictions()).isNotNull();
    }
    
    @Test
    @DisplayName("Should increment queries executed counter")
    void shouldIncrementQueriesExecuted() {
        queryMetrics.recordQueryExecuted();
        queryMetrics.recordQueryExecuted();
        queryMetrics.recordQueryExecuted();
        
        assertThat(queryMetrics.getQueriesExecuted().count()).isEqualTo(3.0);
    }
    
    @Test
    @DisplayName("Should increment queries failed counter")
    void shouldIncrementQueriesFailed() {
        queryMetrics.recordQueryFailed();
        queryMetrics.recordQueryFailed();
        
        assertThat(queryMetrics.getQueriesFailed().count()).isEqualTo(2.0);
    }
    
    @Test
    @DisplayName("Should increment queries timed out counter")
    void shouldIncrementQueriesTimedOut() {
        queryMetrics.recordQueryTimedOut();
        
        assertThat(queryMetrics.getQueriesTimedOut().count()).isEqualTo(1.0);
    }
    
    @Test
    @DisplayName("Should increment hot tier queries counter")
    void shouldIncrementHotTierQueries() {
        queryMetrics.recordHotTierQuery();
        queryMetrics.recordHotTierQuery();
        queryMetrics.recordHotTierQuery();
        queryMetrics.recordHotTierQuery();
        
        assertThat(queryMetrics.getHotTierQueries().count()).isEqualTo(4.0);
    }
    
    @Test
    @DisplayName("Should increment warm tier queries counter")
    void shouldIncrementWarmTierQueries() {
        queryMetrics.recordWarmTierQuery();
        queryMetrics.recordWarmTierQuery();
        
        assertThat(queryMetrics.getWarmTierQueries().count()).isEqualTo(2.0);
    }
    
    @Test
    @DisplayName("Should increment cold tier queries counter")
    void shouldIncrementColdTierQueries() {
        queryMetrics.recordColdTierQuery();
        
        assertThat(queryMetrics.getColdTierQueries().count()).isEqualTo(1.0);
    }
    
    @Test
    @DisplayName("Should increment query errors counter")
    void shouldIncrementQueryErrors() {
        queryMetrics.recordQueryError();
        queryMetrics.recordQueryError();
        queryMetrics.recordQueryError();
        
        assertThat(queryMetrics.getQueryErrors().count()).isEqualTo(3.0);
    }
    
    @Test
    @DisplayName("Should record query execution latency")
    void shouldRecordQueryExecutionLatency() throws InterruptedException {
        Timer.Sample sample = queryMetrics.startQueryTimer();
        Thread.sleep(100); // Simulate query execution
        queryMetrics.recordQueryLatency(sample);
        
        Timer timer = queryMetrics.getQueryExecutionLatency();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(100);
    }
    
    @Test
    @DisplayName("Should record hot tier latency")
    void shouldRecordHotTierLatency() throws InterruptedException {
        Timer.Sample sample = queryMetrics.startHotTierTimer();
        Thread.sleep(50);
        queryMetrics.recordHotTierLatency(sample);
        
        Timer timer = queryMetrics.getHotTierLatency();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(50);
    }
    
    @Test
    @DisplayName("Should record warm tier latency")
    void shouldRecordWarmTierLatency() throws InterruptedException {
        Timer.Sample sample = queryMetrics.startWarmTierTimer();
        Thread.sleep(75);
        queryMetrics.recordWarmTierLatency(sample);
        
        Timer timer = queryMetrics.getWarmTierLatency();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(75);
    }
    
    @Test
    @DisplayName("Should record cold tier latency")
    void shouldRecordColdTierLatency() throws InterruptedException {
        Timer.Sample sample = queryMetrics.startColdTierTimer();
        Thread.sleep(150);
        queryMetrics.recordColdTierLatency(sample);
        
        Timer timer = queryMetrics.getColdTierLatency();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(150);
    }
    
    @Test
    @DisplayName("Should record multiple query latencies and calculate statistics")
    void shouldRecordMultipleLatenciesAndCalculateStatistics() throws InterruptedException {
        // Record multiple queries with varying latencies
        for (int i = 0; i < 10; i++) {
            Timer.Sample sample = queryMetrics.startQueryTimer();
            Thread.sleep(10 + (i * 5)); // Varying latencies: 10ms, 15ms, 20ms, etc.
            queryMetrics.recordQueryLatency(sample);
        }
        
        Timer timer = queryMetrics.getQueryExecutionLatency();
        assertThat(timer.count()).isEqualTo(10);
        assertThat(timer.mean(TimeUnit.MILLISECONDS)).isGreaterThan(0);
        assertThat(timer.max(TimeUnit.MILLISECONDS)).isGreaterThan(timer.mean(TimeUnit.MILLISECONDS));
    }
    
    @Test
    @DisplayName("Should record result size")
    void shouldRecordResultSize() {
        queryMetrics.recordResultSize(100);
        queryMetrics.recordResultSize(500);
        queryMetrics.recordResultSize(1000);
        
        DistributionSummary summary = queryMetrics.getResultSize();
        assertThat(summary.count()).isEqualTo(3);
        assertThat(summary.totalAmount()).isEqualTo(1600.0);
        assertThat(summary.mean()).isCloseTo(533.33, within(0.1));
    }
    
    @Test
    @DisplayName("Should record hot tier result size")
    void shouldRecordHotTierResultSize() {
        queryMetrics.recordHotTierResultSize(250);
        queryMetrics.recordHotTierResultSize(750);
        
        DistributionSummary summary = queryMetrics.getHotTierResultSize();
        assertThat(summary.count()).isEqualTo(2);
        assertThat(summary.totalAmount()).isEqualTo(1000.0);
        assertThat(summary.mean()).isEqualTo(500.0);
    }
    
    @Test
    @DisplayName("Should record warm tier result size")
    void shouldRecordWarmTierResultSize() {
        queryMetrics.recordWarmTierResultSize(5000);
        queryMetrics.recordWarmTierResultSize(10000);
        queryMetrics.recordWarmTierResultSize(15000);
        
        DistributionSummary summary = queryMetrics.getWarmTierResultSize();
        assertThat(summary.count()).isEqualTo(3);
        assertThat(summary.totalAmount()).isEqualTo(30000.0);
        assertThat(summary.mean()).isEqualTo(10000.0);
    }
    
    @Test
    @DisplayName("Should record cold tier result size")
    void shouldRecordColdTierResultSize() {
        queryMetrics.recordColdTierResultSize(50000);
        
        DistributionSummary summary = queryMetrics.getColdTierResultSize();
        assertThat(summary.count()).isEqualTo(1);
        assertThat(summary.totalAmount()).isEqualTo(50000.0);
        assertThat(summary.mean()).isEqualTo(50000.0);
    }
    
    @Test
    @DisplayName("Should track result size distribution with percentiles")
    void shouldTrackResultSizeDistribution() {
        // Record a variety of result sizes
        for (int i = 1; i <= 100; i++) {
            queryMetrics.recordResultSize(i * 10); // 10, 20, 30, ..., 1000
        }
        
        DistributionSummary summary = queryMetrics.getResultSize();
        assertThat(summary.count()).isEqualTo(100);
        assertThat(summary.max()).isEqualTo(1000.0);
        assertThat(summary.mean()).isEqualTo(505.0);
    }
    
    @Test
    @DisplayName("Should increment cache hits counter")
    void shouldIncrementCacheHits() {
        queryMetrics.recordCacheHit();
        queryMetrics.recordCacheHit();
        queryMetrics.recordCacheHit();
        
        assertThat(queryMetrics.getCacheHits().count()).isEqualTo(3.0);
    }
    
    @Test
    @DisplayName("Should increment cache misses counter")
    void shouldIncrementCacheMisses() {
        queryMetrics.recordCacheMiss();
        queryMetrics.recordCacheMiss();
        
        assertThat(queryMetrics.getCacheMisses().count()).isEqualTo(2.0);
    }
    
    @Test
    @DisplayName("Should increment cache evictions counter")
    void shouldIncrementCacheEvictions() {
        queryMetrics.recordCacheEviction();
        
        assertThat(queryMetrics.getCacheEvictions().count()).isEqualTo(1.0);
    }
    
    @Test
    @DisplayName("Should calculate cache hit rate correctly")
    void shouldCalculateCacheHitRate() {
        // Record 7 hits and 3 misses = 70% hit rate
        for (int i = 0; i < 7; i++) {
            queryMetrics.recordCacheHit();
        }
        for (int i = 0; i < 3; i++) {
            queryMetrics.recordCacheMiss();
        }
        
        double hitRate = queryMetrics.getCacheHitRate();
        assertThat(hitRate).isCloseTo(70.0, within(0.01));
    }
    
    @Test
    @DisplayName("Should return zero cache hit rate when no cache operations")
    void shouldReturnZeroCacheHitRateWhenNoOperations() {
        double hitRate = queryMetrics.getCacheHitRate();
        assertThat(hitRate).isEqualTo(0.0);
    }
    
    @Test
    @DisplayName("Should return 100% cache hit rate when all hits")
    void shouldReturn100PercentCacheHitRateWhenAllHits() {
        for (int i = 0; i < 10; i++) {
            queryMetrics.recordCacheHit();
        }
        
        double hitRate = queryMetrics.getCacheHitRate();
        assertThat(hitRate).isEqualTo(100.0);
    }
    
    @Test
    @DisplayName("Should return 0% cache hit rate when all misses")
    void shouldReturn0PercentCacheHitRateWhenAllMisses() {
        for (int i = 0; i < 10; i++) {
            queryMetrics.recordCacheMiss();
        }
        
        double hitRate = queryMetrics.getCacheHitRate();
        assertThat(hitRate).isEqualTo(0.0);
    }
    
    @Test
    @DisplayName("Should track metrics for complex query scenario")
    void shouldTrackMetricsForComplexQueryScenario() throws InterruptedException {
        // Simulate a complex query hitting multiple tiers
        queryMetrics.recordQueryExecuted();
        
        // Check cache first - miss
        queryMetrics.recordCacheMiss();
        
        // Query hot tier
        Timer.Sample hotSample = queryMetrics.startHotTierTimer();
        Thread.sleep(20);
        queryMetrics.recordHotTierLatency(hotSample);
        queryMetrics.recordHotTierQuery();
        queryMetrics.recordHotTierResultSize(150);
        
        // Query warm tier
        Timer.Sample warmSample = queryMetrics.startWarmTierTimer();
        Thread.sleep(30);
        queryMetrics.recordWarmTierLatency(warmSample);
        queryMetrics.recordWarmTierQuery();
        queryMetrics.recordWarmTierResultSize(300);
        
        // Record overall query latency
        Timer.Sample querySample = queryMetrics.startQueryTimer();
        Thread.sleep(50);
        queryMetrics.recordQueryLatency(querySample);
        
        // Record total result size
        queryMetrics.recordResultSize(450);
        
        // Verify all metrics were recorded
        assertThat(queryMetrics.getQueriesExecuted().count()).isEqualTo(1.0);
        assertThat(queryMetrics.getCacheMisses().count()).isEqualTo(1.0);
        assertThat(queryMetrics.getHotTierQueries().count()).isEqualTo(1.0);
        assertThat(queryMetrics.getWarmTierQueries().count()).isEqualTo(1.0);
        assertThat(queryMetrics.getQueryExecutionLatency().count()).isEqualTo(1);
        assertThat(queryMetrics.getHotTierLatency().count()).isEqualTo(1);
        assertThat(queryMetrics.getWarmTierLatency().count()).isEqualTo(1);
        assertThat(queryMetrics.getResultSize().totalAmount()).isEqualTo(450.0);
        assertThat(queryMetrics.getHotTierResultSize().totalAmount()).isEqualTo(150.0);
        assertThat(queryMetrics.getWarmTierResultSize().totalAmount()).isEqualTo(300.0);
    }
    
    @Test
    @DisplayName("Should track metrics for cached query scenario")
    void shouldTrackMetricsForCachedQueryScenario() {
        // Simulate a cached query
        queryMetrics.recordQueryExecuted();
        queryMetrics.recordCacheHit();
        
        // No tier queries needed
        assertThat(queryMetrics.getQueriesExecuted().count()).isEqualTo(1.0);
        assertThat(queryMetrics.getCacheHits().count()).isEqualTo(1.0);
        assertThat(queryMetrics.getHotTierQueries().count()).isEqualTo(0.0);
        assertThat(queryMetrics.getWarmTierQueries().count()).isEqualTo(0.0);
        assertThat(queryMetrics.getColdTierQueries().count()).isEqualTo(0.0);
    }
    
    @Test
    @DisplayName("Should track metrics for failed query scenario")
    void shouldTrackMetricsForFailedQueryScenario() throws InterruptedException {
        // Simulate a failed query
        queryMetrics.recordQueryExecuted();
        queryMetrics.recordCacheMiss();
        
        Timer.Sample sample = queryMetrics.startQueryTimer();
        Thread.sleep(10);
        
        // Query fails
        queryMetrics.recordQueryFailed();
        queryMetrics.recordQueryError();
        queryMetrics.recordQueryLatency(sample);
        
        assertThat(queryMetrics.getQueriesExecuted().count()).isEqualTo(1.0);
        assertThat(queryMetrics.getQueriesFailed().count()).isEqualTo(1.0);
        assertThat(queryMetrics.getQueryErrors().count()).isEqualTo(1.0);
        assertThat(queryMetrics.getQueryExecutionLatency().count()).isEqualTo(1);
    }
    
    @Test
    @DisplayName("Should track metrics for timed out query scenario")
    void shouldTrackMetricsForTimedOutQueryScenario() throws InterruptedException {
        // Simulate a query that times out
        queryMetrics.recordQueryExecuted();
        
        Timer.Sample sample = queryMetrics.startQueryTimer();
        Thread.sleep(10);
        
        // Query times out after 30 seconds (simulated)
        queryMetrics.recordQueryTimedOut();
        queryMetrics.recordQueryLatency(sample);
        
        assertThat(queryMetrics.getQueriesExecuted().count()).isEqualTo(1.0);
        assertThat(queryMetrics.getQueriesTimedOut().count()).isEqualTo(1.0);
    }
}
