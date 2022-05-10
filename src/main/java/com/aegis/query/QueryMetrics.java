package com.aegis.query;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Metrics collector for query execution operations
 * Tracks query execution performance, errors, and tier-specific metrics
 */
@Component
public class QueryMetrics {
    
    @Autowired
    private MeterRegistry meterRegistry;
    
    private Counter queriesExecuted;
    private Counter queriesFailed;
    private Counter hotTierQueries;
    private Counter warmTierQueries;
    private Counter coldTierQueries;
    private Counter queryErrors;
    private Timer queryExecutionLatency;
    private Timer hotTierLatency;
    private Timer warmTierLatency;
    private Timer coldTierLatency;
    
    @PostConstruct
    public void init() {
        queriesExecuted = Counter.builder("aegis.query.executed")
            .description("Total number of queries executed")
            .register(meterRegistry);
        
        queriesFailed = Counter.builder("aegis.query.failed")
            .description("Total number of queries that failed")
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
        
        queryExecutionLatency = Timer.builder("aegis.query.execution.latency")
            .description("Latency of overall query execution")
            .register(meterRegistry);
        
        hotTierLatency = Timer.builder("aegis.query.hot.tier.latency")
            .description("Latency of hot tier query execution")
            .register(meterRegistry);
        
        warmTierLatency = Timer.builder("aegis.query.warm.tier.latency")
            .description("Latency of warm tier query execution")
            .register(meterRegistry);
        
        coldTierLatency = Timer.builder("aegis.query.cold.tier.latency")
            .description("Latency of cold tier query execution")
            .register(meterRegistry);
    }
    
    public void recordQueryExecuted() {
        queriesExecuted.increment();
    }
    
    public void recordQueryFailed() {
        queriesFailed.increment();
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
}
