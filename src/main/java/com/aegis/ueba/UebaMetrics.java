package com.aegis.ueba;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Metrics collector for UEBA scoring operations
 */
@Component
public class UebaMetrics {
    
    @Autowired
    private MeterRegistry meterRegistry;
    
    private Counter eventsScored;
    private Counter anomaliesDetected;
    private Counter profilesCreated;
    private Counter profilesUpdated;
    private Timer scoringLatency;
    private Timer profileUpdateLatency;
    
    @PostConstruct
    public void init() {
        eventsScored = Counter.builder("aegis.ueba.events.scored")
            .description("Total number of events scored by UEBA")
            .register(meterRegistry);
        
        anomaliesDetected = Counter.builder("aegis.ueba.anomalies.detected")
            .description("Total number of anomalies detected")
            .register(meterRegistry);
        
        profilesCreated = Counter.builder("aegis.ueba.profiles.created")
            .description("Total number of user profiles created")
            .register(meterRegistry);
        
        profilesUpdated = Counter.builder("aegis.ueba.profiles.updated")
            .description("Total number of user profiles updated")
            .register(meterRegistry);
        
        scoringLatency = Timer.builder("aegis.ueba.scoring.latency")
            .description("Latency of UEBA scoring operations")
            .register(meterRegistry);
        
        profileUpdateLatency = Timer.builder("aegis.ueba.profile.update.latency")
            .description("Latency of profile update operations")
            .register(meterRegistry);
    }
    
    public void recordEventScored() {
        eventsScored.increment();
    }
    
    public void recordAnomalyDetected() {
        anomaliesDetected.increment();
    }
    
    public void recordProfileCreated() {
        profilesCreated.increment();
    }
    
    public void recordProfileUpdated() {
        profilesUpdated.increment();
    }
    
    public Timer.Sample startScoringTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void recordScoringLatency(Timer.Sample sample) {
        sample.stop(scoringLatency);
    }
    
    public Timer.Sample startProfileUpdateTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void recordProfileUpdateLatency(Timer.Sample sample) {
        sample.stop(profileUpdateLatency);
    }
}
