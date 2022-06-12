package com.aegis.storage.hot;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Metrics collector for OpenSearch indexing operations
 */
@Component
public class IndexingMetrics {
    
    @Autowired
    private MeterRegistry meterRegistry;
    
    private Counter documentsIndexed;
    private Counter indexingErrors;
    private Counter retries;
    private Timer indexingLatency;
    private Timer bulkLatency;
    
    @PostConstruct
    public void init() {
        documentsIndexed = Counter.builder("aegis.storage.documents.indexed")
            .description("Total number of documents indexed to OpenSearch")
            .register(meterRegistry);
        
        indexingErrors = Counter.builder("aegis.storage.indexing.errors")
            .description("Total number of indexing errors")
            .register(meterRegistry);
        
        retries = Counter.builder("aegis.storage.indexing.retries")
            .description("Total number of indexing retries")
            .register(meterRegistry);
        
        indexingLatency = Timer.builder("aegis.storage.indexing.latency")
            .description("Latency of individual document indexing")
            .register(meterRegistry);
        
        bulkLatency = Timer.builder("aegis.storage.bulk.latency")
            .description("Latency of bulk indexing operations")
            .register(meterRegistry);
    }
    
    public void recordDocumentIndexed() {
        documentsIndexed.increment();
    }
    
    public void recordDocumentsIndexed(long count) {
        documentsIndexed.increment(count);
    }
    
    public void recordIndexingError() {
        indexingErrors.increment();
    }
    
    public void recordRetry() {
        retries.increment();
    }
    
    public Timer.Sample startIndexingTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void recordIndexingLatency(Timer.Sample sample) {
        sample.stop(indexingLatency);
    }
    
    public void recordBulkLatency(long millis) {
        bulkLatency.record(millis, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
}
