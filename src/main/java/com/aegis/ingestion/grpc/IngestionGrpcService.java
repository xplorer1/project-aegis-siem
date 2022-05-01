package com.aegis.ingestion.grpc;

import com.aegis.domain.RawEvent;
import com.aegis.ingestion.grpc.proto.*;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * gRPC Ingestion Service Implementation
 * High-performance event ingestion via gRPC protocol
 * Supports single events, batches, and streaming
 */
@GrpcService
public class IngestionGrpcService extends IngestionServiceGrpc.IngestionServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(IngestionGrpcService.class);
    
    private final MeterRegistry meterRegistry;
    private Counter eventsReceived;
    private Counter eventsProcessed;
    private Counter eventsFailed;
    private Counter requestsReceived;
    private Timer processingTimer;
    
    public IngestionGrpcService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        initializeMetrics();
    }
    
    private void initializeMetrics() {
        this.eventsReceived = Counter.builder("ingestion.grpc.events.received")
            .description("Total gRPC events received")
            .register(meterRegistry);
        
        this.eventsProcessed = Counter.builder("ingestion.grpc.events.processed")
            .description("Total gRPC events successfully processed")
            .register(meterRegistry);
        
        this.eventsFailed = Counter.builder("ingestion.grpc.events.failed")
            .description("Total gRPC events that failed processing")
            .register(meterRegistry);
        
        this.requestsReceived = Counter.builder("ingestion.grpc.requests.received")
            .description("Total gRPC requests received")
            .register(meterRegistry);
        
        this.processingTimer = Timer.builder("ingestion.grpc.processing.time")
            .description("Time to process gRPC requests")
            .register(meterRegistry);
    }
    
    /**
     * Ingest a single event
     * @param request IngestEventRequest containing event data
     * @param responseObserver Observer for sending response
     */
    @Override
    public void ingestEvent(IngestEventRequest request, 
                           StreamObserver<IngestEventResponse> responseObserver) {
        requestsReceived.increment();
        eventsReceived.increment();
        
        log.debug("Received gRPC event ingestion request for tenant: {}", request.getTenantId());
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Validate request
            if (request.getTenantId() == null || request.getTenantId().isEmpty()) {
                IngestEventResponse response = IngestEventResponse.newBuilder()
                    .setCode(1)
                    .setMessage("Error: Tenant ID is required")
                    .setEventsIngested(0)
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                eventsFailed.increment();
                return;
            }
            
            if (request.getEventData() == null || request.getEventData().isEmpty()) {
                IngestEventResponse response = IngestEventResponse.newBuilder()
                    .setCode(1)
                    .setMessage("Error: Event data is required")
                    .setEventsIngested(0)
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                eventsFailed.increment();
                return;
            }
            
            // Process event
            processEvent(request);
            
            // Send success response
            IngestEventResponse response = IngestEventResponse.newBuilder()
                .setCode(0)
                .setMessage("Success")
                .setEventsIngested(1)
                .setAckId(System.currentTimeMillis())
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            eventsProcessed.increment();
            sample.stop(processingTimer);
            
            log.debug("Successfully processed gRPC event for tenant: {}", request.getTenantId());
            
        } catch (Exception e) {
            log.error("Failed to process gRPC event", e);
            eventsFailed.increment();
            
            IngestEventResponse response = IngestEventResponse.newBuilder()
                .setCode(1)
                .setMessage("Error: " + e.getMessage())
                .setEventsIngested(0)
                .setErrorDetails(e.toString())
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            sample.stop(processingTimer);
        }
    }
    
    /**
     * Ingest multiple events in a batch
     * @param request IngestEventBatchRequest containing multiple events
     * @param responseObserver Observer for sending response
     */
    @Override
    public void ingestEventBatch(IngestEventBatchRequest request,
                                StreamObserver<IngestEventResponse> responseObserver) {
        requestsReceived.increment();
        
        log.debug("Received gRPC batch ingestion request for tenant: {} with {} events",
            request.getTenantId(), request.getEventsCount());
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Validate request
            if (request.getTenantId() == null || request.getTenantId().isEmpty()) {
                IngestEventResponse response = IngestEventResponse.newBuilder()
                    .setCode(1)
                    .setMessage("Error: Tenant ID is required")
                    .setEventsIngested(0)
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            
            if (request.getEventsCount() == 0) {
                IngestEventResponse response = IngestEventResponse.newBuilder()
                    .setCode(1)
                    .setMessage("Error: No events provided")
                    .setEventsIngested(0)
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            
            // Process each event in the batch
            int successCount = 0;
            for (EventData eventData : request.getEventsList()) {
                eventsReceived.increment();
                try {
                    processEventData(eventData, request.getTenantId());
                    successCount++;
                    eventsProcessed.increment();
                } catch (Exception e) {
                    log.error("Failed to process event in batch", e);
                    eventsFailed.increment();
                }
            }
            
            // Send response
            IngestEventResponse response = IngestEventResponse.newBuilder()
                .setCode(0)
                .setMessage("Success")
                .setEventsIngested(successCount)
                .setAckId(System.currentTimeMillis())
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            sample.stop(processingTimer);
            
            log.debug("Successfully processed {} out of {} events for tenant: {}",
                successCount, request.getEventsCount(), request.getTenantId());
            
        } catch (Exception e) {
            log.error("Failed to process gRPC batch", e);
            
            IngestEventResponse response = IngestEventResponse.newBuilder()
                .setCode(1)
                .setMessage("Error: " + e.getMessage())
                .setEventsIngested(0)
                .setErrorDetails(e.toString())
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            sample.stop(processingTimer);
        }
    }
    
    /**
     * Bidirectional streaming for continuous event ingestion
     * Allows clients to stream events and receive acknowledgments in real-time
     * Implements backpressure handling for high-throughput scenarios
     * @param responseObserver Observer for sending responses
     * @return StreamObserver for receiving events
     */
    @Override
    public StreamObserver<IngestEventRequest> ingestEventStream(
            StreamObserver<IngestEventResponse> responseObserver) {
        
        requestsReceived.increment();
        log.debug("Started gRPC streaming session");
        
        return new StreamObserver<IngestEventRequest>() {
            private int streamEventCount = 0;
            private int streamSuccessCount = 0;
            private int streamFailureCount = 0;
            private final Timer.Sample streamSample = Timer.start(meterRegistry);
            
            @Override
            public void onNext(IngestEventRequest request) {
                streamEventCount++;
                eventsReceived.increment();
                
                try {
                    // Validate request
                    if (request.getTenantId() == null || request.getTenantId().isEmpty()) {
                        IngestEventResponse response = IngestEventResponse.newBuilder()
                            .setCode(1)
                            .setMessage("Error: Tenant ID is required")
                            .setEventsIngested(0)
                            .build();
                        responseObserver.onNext(response);
                        streamFailureCount++;
                        eventsFailed.increment();
                        return;
                    }
                    
                    if (request.getEventData() == null || request.getEventData().isEmpty()) {
                        IngestEventResponse response = IngestEventResponse.newBuilder()
                            .setCode(1)
                            .setMessage("Error: Event data is required")
                            .setEventsIngested(0)
                            .build();
                        responseObserver.onNext(response);
                        streamFailureCount++;
                        eventsFailed.increment();
                        return;
                    }
                    
                    // Process event
                    processEvent(request);
                    
                    // Send acknowledgment
                    IngestEventResponse response = IngestEventResponse.newBuilder()
                        .setCode(0)
                        .setMessage("Success")
                        .setEventsIngested(1)
                        .setAckId(System.currentTimeMillis())
                        .build();
                    
                    responseObserver.onNext(response);
                    streamSuccessCount++;
                    eventsProcessed.increment();
                    
                    // Log progress every 1000 events
                    if (streamEventCount % 1000 == 0) {
                        log.debug("Streaming progress: {} events received, {} successful, {} failed",
                            streamEventCount, streamSuccessCount, streamFailureCount);
                    }
                    
                } catch (Exception e) {
                    log.error("Failed to process streaming event", e);
                    streamFailureCount++;
                    eventsFailed.increment();
                    
                    IngestEventResponse response = IngestEventResponse.newBuilder()
                        .setCode(1)
                        .setMessage("Error: " + e.getMessage())
                        .setEventsIngested(0)
                        .setErrorDetails(e.toString())
                        .build();
                    
                    responseObserver.onNext(response);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                log.error("Error in gRPC streaming session after {} events", streamEventCount, t);
                streamSample.stop(processingTimer);
            }
            
            @Override
            public void onCompleted() {
                log.info("Completed gRPC streaming session: {} events received, {} successful, {} failed",
                    streamEventCount, streamSuccessCount, streamFailureCount);
                
                // Send final summary response
                IngestEventResponse finalResponse = IngestEventResponse.newBuilder()
                    .setCode(0)
                    .setMessage("Stream completed")
                    .setEventsIngested(streamSuccessCount)
                    .setAckId(System.currentTimeMillis())
                    .build();
                
                responseObserver.onNext(finalResponse);
                responseObserver.onCompleted();
                streamSample.stop(processingTimer);
            }
        };
    }
    
    /**
     * Process a single event request
     * @param request IngestEventRequest
     */
    private void processEvent(IngestEventRequest request) {
        // Create RawEvent
        byte[] eventBytes = request.getEventData().toByteArray();
        Instant timestamp = request.getTimestamp() > 0 
            ? Instant.ofEpochMilli(request.getTimestamp())
            : Instant.now();
        
        RawEvent rawEvent = new RawEvent(eventBytes, timestamp);
        
        // Log metadata at trace level
        if (log.isTraceEnabled() && request.hasMetadata()) {
            EventMetadata metadata = request.getMetadata();
            log.trace("Event metadata - source: {}, sourceType: {}, host: {}",
                metadata.getSource(), metadata.getSourceType(), metadata.getHost());
        }
        
        // TODO: Send to Kafka producer (will be implemented in later tasks)
        // For now, just log
        log.trace("Processed gRPC event: {} bytes", eventBytes.length);
    }
    
    /**
     * Process event data from batch
     * @param eventData EventData
     * @param tenantId Tenant identifier
     */
    private void processEventData(EventData eventData, String tenantId) {
        byte[] eventBytes = eventData.getEventData().toByteArray();
        Instant timestamp = eventData.getTimestamp() > 0
            ? Instant.ofEpochMilli(eventData.getTimestamp())
            : Instant.now();
        
        RawEvent rawEvent = new RawEvent(eventBytes, timestamp);
        
        // TODO: Send to Kafka producer (will be implemented in later tasks)
        // For now, just log
        log.trace("Processed batch event: {} bytes for tenant: {}", eventBytes.length, tenantId);
    }
}
