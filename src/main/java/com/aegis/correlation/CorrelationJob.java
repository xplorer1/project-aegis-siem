package com.aegis.correlation;

import com.aegis.domain.Alert;
import com.aegis.domain.OcsfEvent;
import com.aegis.enrichment.TipClientRestImpl;
import com.aegis.enrichment.TipEnrichmentFunction;
import com.aegis.ueba.UebaScoringFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

/**
 * Main Flink correlation job that processes normalized events through
 * the threat detection pipeline.
 * 
 * Pipeline stages:
 * 1. Consume normalized events from Kafka
 * 2. Enrich with threat intelligence (async I/O)
 * 3. Apply Sigma rules for threat detection
 * 4. Apply UEBA scoring for anomaly detection
 * 5. Produce alerts to Kafka
 */
@Component
public class CorrelationJob {
    
    private static final Logger log = LoggerFactory.getLogger(CorrelationJob.class);
    
    @Autowired
    private StreamExecutionEnvironment env;
    
    @Autowired
    private KafkaSource<OcsfEvent> kafkaSource;
    
    @Autowired
    private WatermarkStrategy<OcsfEvent> watermarkStrategy;
    
    @Autowired
    private KafkaSink<Alert> kafkaSink;
    
    @Autowired
    private TipClientRestImpl tipClient;
    
    @Autowired
    private com.aegis.enrichment.EnrichmentMetrics enrichmentMetrics;
    
    @Autowired
    private UebaScoringFunction uebaScoringFunction;
    
    /**
     * Initialize and start the Flink job after Spring context is ready.
     */
    @PostConstruct
    public void startJob() {
        try {
            log.info("Starting Flink correlation job...");
            
            // Build the processing pipeline
            DataStream<OcsfEvent> eventStream = buildPipeline();
            
            // Execute the job
            // Note: In production, this would be submitted to a Flink cluster
            // For now, we'll execute in embedded mode
            // env.execute("AEGIS Correlation Engine");
            
            log.info("Flink correlation job configured successfully");
            
        } catch (Exception e) {
            log.error("Failed to start Flink correlation job", e);
            throw new RuntimeException("Failed to start Flink correlation job", e);
        }
    }
    
    /**
     * Build the complete processing pipeline.
     * 
     * @return The enriched event stream
     */
    private DataStream<OcsfEvent> buildPipeline() {
        // 1. Create source stream from Kafka
        DataStream<OcsfEvent> sourceStream = env
            .fromSource(kafkaSource, watermarkStrategy, "Kafka Source")
            .name("Normalized Events Source")
            .uid("normalized-events-source");
        
        // 2. Apply threat intelligence enrichment using Async I/O
        // Async I/O allows non-blocking external lookups with high throughput
        // Capacity: 100 concurrent requests
        // Timeout: 1000ms per request
        // Ordered: maintain event order
        DataStream<OcsfEvent> enrichedStream = AsyncDataStream.orderedWait(
            sourceStream,
            new TipEnrichmentFunction(tipClient, enrichmentMetrics),
            1000,  // Timeout in milliseconds
            TimeUnit.MILLISECONDS,
            100    // Capacity (max concurrent async requests)
        ).name("TIP Enrichment")
         .uid("tip-enrichment");
        
        // 3. Apply UEBA scoring for anomaly detection
        SingleOutputStreamOperator<OcsfEvent> uebaStream = enrichedStream
            .process(uebaScoringFunction)
            .name("UEBA Scoring")
            .uid("ueba-scoring");
        
        // Extract UEBA alerts from side output
        DataStream<Alert> uebaAlerts = uebaStream
            .getSideOutput(UebaScoringFunction.ALERT_OUTPUT)
            .name("UEBA Alerts");
        
        // 4. TODO: Apply Sigma rules (will be added in future tasks)
        // DataStream<Alert> sigmaAlerts = uebaStream
        //     .flatMap(new SigmaRuleProcessor())
        //     .name("Sigma Rule Detection");
        
        // 5. Sink UEBA alerts to Kafka
        uebaAlerts
            .sinkTo(kafkaSink)
            .name("Alert Sink")
            .uid("alert-sink");
        
        log.info("Pipeline configured with TIP enrichment and UEBA scoring");
        
        return uebaStream;
    }
}
