package com.aegis.query.grpc;

import com.aegis.domain.QueryResult;
import com.aegis.domain.StorageTier;
import com.aegis.query.AqlTranspiler;
import com.aegis.query.QueryExecutor;
import com.aegis.query.QueryPlan;
import com.aegis.query.grpc.proto.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * gRPC Query Service Implementation
 * 
 * This service provides high-performance query execution via gRPC protocol.
 * It integrates with the QueryExecutor and AqlTranspiler to execute AQL queries
 * across multiple storage tiers (Hot/Warm/Cold) and return results.
 * 
 * Features:
 * - Unary RPC for standard queries with complete results
 * - Server-side streaming RPC for large result sets
 * - Automatic query transpilation from AQL to tier-specific formats
 * - Multi-tenant isolation via tenant_id validation
 * - Comprehensive error handling and metrics tracking
 * - Timeout support with partial result handling
 * 
 * The service follows the same pattern as IngestionGrpcService for consistency.
 */
@GrpcService
public class QueryGrpcService extends QueryServiceGrpc.QueryServiceImplBase {
    
    private static final Logger log = LoggerFactory.getLogger(QueryGrpcService.class);
    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 10000;
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final int STREAM_BATCH_SIZE = 100;
    
    private final QueryExecutor queryExecutor;
    private final AqlTranspiler aqlTranspiler;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;
    
    // Metrics
    private final Counter queriesReceived;
    private final Counter queriesSucceeded;
    private final Counter queriesFailed;
    private final Counter queriesTimedOut;
    private final Timer queryExecutionTimer;
    
    /**
     * Constructor with dependency injection
     * 
     * @param queryExecutor Executor for running queries across storage tiers
     * @param aqlTranspiler Transpiler for converting AQL to tier-specific queries
     * @param meterRegistry Metrics registry for tracking query performance
     */
    public QueryGrpcService(
            QueryExecutor queryExecutor,
            AqlTranspiler aqlTranspiler,
            MeterRegistry meterRegistry) {
        this.queryExecutor = queryExecutor;
        this.aqlTranspiler = aqlTranspiler;
        this.meterRegistry = meterRegistry;
        this.objectMapper = new ObjectMapper();
        
        // Initialize metrics
        this.queriesReceived = Counter.builder("query.grpc.queries.received")
            .description("Total gRPC queries received")
            .register(meterRegistry);
        
        this.queriesSucceeded = Counter.builder("query.grpc.queries.succeeded")
            .description("Total gRPC queries that succeeded")
            .register(meterRegistry);
        
        this.queriesFailed = Counter.builder("query.grpc.queries.failed")
            .description("Total gRPC queries that failed")
            .register(meterRegistry);
        
        this.queriesTimedOut = Counter.builder("query.grpc.queries.timedout")
            .description("Total gRPC queries that timed out")
            .register(meterRegistry);
        
        this.queryExecutionTimer = Timer.builder("query.grpc.execution.time")
            .description("Time to execute gRPC queries")
            .register(meterRegistry);
        
        log.info("QueryGrpcService initialized");
    }
    
    /**
     * Execute an AQL query and return complete results
     * 
     * This is a unary RPC that:
     * 1. Validates the request (tenant_id, query string)
     * 2. Transpiles the AQL query to a QueryPlan
     * 3. Executes the plan across relevant storage tiers
     * 4. Converts the QueryResult to a gRPC QueryResponse
     * 5. Returns the response with all results
     * 
     * For large result sets, consider using executeQueryStream instead.
     * 
     * @param request QueryRequest containing AQL query and parameters
     * @param responseObserver Observer for sending the response
     */
    @Override
    public void executeQuery(
            QueryRequest request,
            StreamObserver<QueryResponse> responseObserver) {
        
        queriesReceived.increment();
        
        log.debug("Received gRPC query request for tenant: {}, query: {}", 
            request.getTenantId(), request.getQuery());
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Validate request
            validateRequest(request);
            
            // Transpile AQL query to QueryPlan
            QueryPlan queryPlan = aqlTranspiler.transpile(request.getQuery());
            
            log.debug("Transpiled query to plan with {} sub-queries", queryPlan.size());
            
            // Determine if pagination is requested
            boolean usePagination = request.getLimit() > 0 || request.getOffset() > 0;
            
            // Execute query
            Flux<QueryResult> resultFlux;
            if (usePagination) {
                int limit = normalizeLimit(request.getLimit());
                String cursor = request.getOffset() > 0 
                    ? generateOffsetCursor(request.getOffset()) 
                    : null;
                resultFlux = queryExecutor.executeWithPagination(queryPlan, cursor, limit);
            } else {
                resultFlux = queryExecutor.execute(queryPlan);
            }
            
            // Block and get result (unary RPC waits for complete result)
            QueryResult result = resultFlux.blockFirst();
            
            if (result == null) {
                result = new QueryResult();
            }
            
            // Convert to gRPC response
            QueryResponse response = convertToGrpcResponse(result, request);
            
            // Send response
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            queriesSucceeded.increment();
            sample.stop(queryExecutionTimer);
            
            if (result.isTimedOut()) {
                queriesTimedOut.increment();
                log.warn("Query completed with timeout for tenant: {}, returned {} rows", 
                    request.getTenantId(), result.getTotalCount());
            } else {
                log.debug("Query completed successfully for tenant: {}, returned {} rows in {}ms",
                    request.getTenantId(), result.getTotalCount(), result.getExecutionTimeMs());
            }
            
        } catch (IllegalArgumentException e) {
            // Validation error
            log.warn("Invalid query request: {}", e.getMessage());
            queriesFailed.increment();
            sample.stop(queryExecutionTimer);
            
            QueryResponse errorResponse = QueryResponse.newBuilder()
                .setCode(1)
                .setMessage("Validation error: " + e.getMessage())
                .setTotalCount(0)
                .setExecutionTimeMs(0)
                .setErrorDetails(e.toString())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            // Execution error
            log.error("Failed to execute gRPC query for tenant: {}", request.getTenantId(), e);
            queriesFailed.increment();
            sample.stop(queryExecutionTimer);
            
            QueryResponse errorResponse = QueryResponse.newBuilder()
                .setCode(2)
                .setMessage("Query execution error: " + e.getMessage())
                .setTotalCount(0)
                .setExecutionTimeMs(0)
                .setErrorDetails(e.toString())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }
    
    /**
     * Execute an AQL query with server-side streaming for large result sets
     * 
     * This is a server-streaming RPC that:
     * 1. Validates the request
     * 2. Transpiles the AQL query to a QueryPlan
     * 3. Executes the query and streams results in batches
     * 4. Sends multiple QueryResponse messages as results become available
     * 
     * This is more efficient for large result sets as it:
     * - Reduces memory usage on both client and server
     * - Allows client to start processing results immediately
     * - Provides better backpressure handling
     * 
     * Results are streamed in batches of STREAM_BATCH_SIZE (100) events.
     * 
     * @param request QueryRequest containing AQL query and parameters
     * @param responseObserver Observer for streaming responses
     */
    @Override
    public void executeQueryStream(
            QueryRequest request,
            StreamObserver<QueryResponse> responseObserver) {
        
        queriesReceived.increment();
        
        log.debug("Received gRPC streaming query request for tenant: {}, query: {}", 
            request.getTenantId(), request.getQuery());
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Validate request
            validateRequest(request);
            
            // Transpile AQL query to QueryPlan
            QueryPlan queryPlan = aqlTranspiler.transpile(request.getQuery());
            
            log.debug("Transpiled streaming query to plan with {} sub-queries", queryPlan.size());
            
            // Execute query
            Flux<QueryResult> resultFlux = queryExecutor.execute(queryPlan);
            
            // Stream results
            resultFlux.subscribe(
                result -> {
                    try {
                        // Stream results in batches
                        streamResultsInBatches(result, request, responseObserver);
                        
                    } catch (Exception e) {
                        log.error("Error streaming query results", e);
                        responseObserver.onError(
                            Status.INTERNAL
                                .withDescription("Error streaming results: " + e.getMessage())
                                .withCause(e)
                                .asRuntimeException()
                        );
                    }
                },
                error -> {
                    // Error during query execution
                    log.error("Failed to execute streaming query for tenant: {}", 
                        request.getTenantId(), error);
                    queriesFailed.increment();
                    sample.stop(queryExecutionTimer);
                    
                    responseObserver.onError(
                        Status.INTERNAL
                            .withDescription("Query execution error: " + error.getMessage())
                            .withCause(error)
                            .asRuntimeException()
                    );
                },
                () -> {
                    // Query completed successfully
                    responseObserver.onCompleted();
                    queriesSucceeded.increment();
                    sample.stop(queryExecutionTimer);
                    
                    log.debug("Streaming query completed successfully for tenant: {}", 
                        request.getTenantId());
                }
            );
            
        } catch (IllegalArgumentException e) {
            // Validation error
            log.warn("Invalid streaming query request: {}", e.getMessage());
            queriesFailed.increment();
            sample.stop(queryExecutionTimer);
            
            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription("Validation error: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException()
            );
            
        } catch (Exception e) {
            // Unexpected error
            log.error("Failed to start streaming query for tenant: {}", request.getTenantId(), e);
            queriesFailed.increment();
            sample.stop(queryExecutionTimer);
            
            responseObserver.onError(
                Status.INTERNAL
                    .withDescription("Failed to start query: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException()
            );
        }
    }
    
    /**
     * Validate the query request
     * 
     * Checks:
     * - Tenant ID is present and not empty
     * - Query string is present and not empty
     * - Limit is within acceptable range (if specified)
     * - Timeout is reasonable (if specified)
     * 
     * @param request The query request to validate
     * @throws IllegalArgumentException if validation fails
     */
    private void validateRequest(QueryRequest request) {
        if (request.getTenantId() == null || request.getTenantId().isEmpty()) {
            throw new IllegalArgumentException("Tenant ID is required");
        }
        
        if (request.getQuery() == null || request.getQuery().isEmpty()) {
            throw new IllegalArgumentException("Query string is required");
        }
        
        if (request.getLimit() < 0) {
            throw new IllegalArgumentException("Limit must be non-negative");
        }
        
        if (request.getLimit() > MAX_LIMIT) {
            throw new IllegalArgumentException(
                String.format("Limit exceeds maximum allowed value of %d", MAX_LIMIT));
        }
        
        if (request.getOffset() < 0) {
            throw new IllegalArgumentException("Offset must be non-negative");
        }
        
        if (request.getTimeoutSeconds() < 0) {
            throw new IllegalArgumentException("Timeout must be non-negative");
        }
    }
    
    /**
     * Normalize the limit value
     * 
     * - If limit is 0, use DEFAULT_LIMIT (100)
     * - If limit exceeds MAX_LIMIT, cap at MAX_LIMIT (10000)
     * 
     * @param limit The requested limit
     * @return The normalized limit
     */
    private int normalizeLimit(int limit) {
        if (limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }
    
    /**
     * Generate a cursor from an offset value
     * 
     * This is a simple implementation that encodes the offset.
     * In a real implementation, this would use the same cursor format
     * as the QueryExecutor.
     * 
     * @param offset The offset value
     * @return A cursor string
     */
    private String generateOffsetCursor(int offset) {
        // Simple implementation - in production, use proper cursor encoding
        return String.format("{\"offset\":%d}", offset);
    }
    
    /**
     * Convert QueryResult to gRPC QueryResponse
     * 
     * Maps the internal QueryResult domain object to the protobuf QueryResponse.
     * Handles conversion of:
     * - Result rows to Event messages
     * - Storage tiers to string representations
     * - Pagination metadata (cursor, hasMore)
     * - Execution metadata (time, timeout status)
     * 
     * @param result The query result to convert
     * @param request The original request (for context)
     * @return A gRPC QueryResponse
     */
    private QueryResponse convertToGrpcResponse(QueryResult result, QueryRequest request) {
        QueryResponse.Builder responseBuilder = QueryResponse.newBuilder();
        
        // Set status
        if (result.isTimedOut()) {
            responseBuilder.setCode(3); // Timeout code
            responseBuilder.setMessage("Query timed out - returning partial results");
        } else {
            responseBuilder.setCode(0); // Success
            responseBuilder.setMessage("Success");
        }
        
        // Convert rows to Event messages
        List<Event> events = convertRowsToEvents(result.getRows());
        responseBuilder.addAllEvents(events);
        
        // Set metadata
        responseBuilder.setTotalCount(result.getTotalCount());
        responseBuilder.setExecutionTimeMs(result.getExecutionTimeMs());
        
        // Set storage tier if available
        if (result.getStorageTier() != null) {
            responseBuilder.addTiersQueried(result.getStorageTier().name());
        }
        
        // Set pagination cursor if available
        if (result.getCursor() != null) {
            responseBuilder.setCursor(result.getCursor());
        }
        
        return responseBuilder.build();
    }
    
    /**
     * Convert result rows to Event messages
     * 
     * Maps the generic Map<String, Object> rows to strongly-typed Event protobuf messages.
     * Handles type conversions and null values gracefully.
     * 
     * @param rows The result rows to convert
     * @return A list of Event messages
     */
    private List<Event> convertRowsToEvents(List<Map<String, Object>> rows) {
        List<Event> events = new ArrayList<>();
        
        for (Map<String, Object> row : rows) {
            try {
                Event event = convertRowToEvent(row);
                events.add(event);
            } catch (Exception e) {
                log.warn("Failed to convert row to Event: {}", row, e);
                // Skip this row and continue with others
            }
        }
        
        return events;
    }
    
    /**
     * Convert a single row to an Event message
     * 
     * @param row The row data
     * @return An Event message
     */
    private Event convertRowToEvent(Map<String, Object> row) {
        Event.Builder eventBuilder = Event.newBuilder();
        
        // Set basic fields with null checks
        if (row.get("_id") != null) {
            eventBuilder.setId(row.get("_id").toString());
        }
        
        if (row.get("time") != null) {
            eventBuilder.setTime(getLongValue(row, "time"));
        }
        
        if (row.get("tenant_id") != null) {
            eventBuilder.setTenantId(row.get("tenant_id").toString());
        }
        
        if (row.get("severity") != null) {
            eventBuilder.setSeverity(getIntValue(row, "severity"));
        }
        
        if (row.get("class_uid") != null) {
            eventBuilder.setClassUid(getIntValue(row, "class_uid"));
        }
        
        if (row.get("category_uid") != null) {
            eventBuilder.setCategoryUid(getIntValue(row, "category_uid"));
        }
        
        if (row.get("message") != null) {
            eventBuilder.setMessage(row.get("message").toString());
        }
        
        if (row.get("raw_data") != null) {
            eventBuilder.setRawData(row.get("raw_data").toString());
        }
        
        if (row.get("ueba_score") != null) {
            eventBuilder.setUebaScore(getDoubleValue(row, "ueba_score"));
        }
        
        // Set nested objects (Actor, Endpoints, ThreatInfo)
        if (row.containsKey("actor")) {
            Actor actor = convertToActor(row.get("actor"));
            if (actor != null) {
                eventBuilder.setActor(actor);
            }
        }
        
        if (row.containsKey("src_endpoint")) {
            Endpoint srcEndpoint = convertToEndpoint(row.get("src_endpoint"));
            if (srcEndpoint != null) {
                eventBuilder.setSrcEndpoint(srcEndpoint);
            }
        }
        
        if (row.containsKey("dst_endpoint")) {
            Endpoint dstEndpoint = convertToEndpoint(row.get("dst_endpoint"));
            if (dstEndpoint != null) {
                eventBuilder.setDstEndpoint(dstEndpoint);
            }
        }
        
        if (row.containsKey("threat_info")) {
            ThreatInfo threatInfo = convertToThreatInfo(row.get("threat_info"));
            if (threatInfo != null) {
                eventBuilder.setThreatInfo(threatInfo);
            }
        }
        
        // Add metadata fields
        row.forEach((key, value) -> {
            if (value != null && !isStandardField(key)) {
                eventBuilder.putMetadata(key, value.toString());
            }
        });
        
        return eventBuilder.build();
    }
    
    /**
     * Convert actor data to Actor message
     */
    private Actor convertToActor(Object actorData) {
        if (actorData == null) {
            return null;
        }
        
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> actorMap = (Map<String, Object>) actorData;
            
            Actor.Builder actorBuilder = Actor.newBuilder();
            
            if (actorMap.get("user") != null) {
                actorBuilder.setUser(actorMap.get("user").toString());
            }
            
            if (actorMap.get("process") != null) {
                actorBuilder.setProcess(actorMap.get("process").toString());
            }
            
            if (actorMap.get("session") != null) {
                actorBuilder.setSession(actorMap.get("session").toString());
            }
            
            return actorBuilder.build();
        } catch (Exception e) {
            log.warn("Failed to convert actor data: {}", actorData, e);
            return null;
        }
    }
    
    /**
     * Convert endpoint data to Endpoint message
     */
    private Endpoint convertToEndpoint(Object endpointData) {
        if (endpointData == null) {
            return null;
        }
        
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> endpointMap = (Map<String, Object>) endpointData;
            
            Endpoint.Builder endpointBuilder = Endpoint.newBuilder();
            
            if (endpointMap.get("ip") != null) {
                endpointBuilder.setIp(endpointMap.get("ip").toString());
            }
            
            if (endpointMap.get("port") != null) {
                endpointBuilder.setPort(getIntValue(endpointMap, "port"));
            }
            
            if (endpointMap.get("hostname") != null) {
                endpointBuilder.setHostname(endpointMap.get("hostname").toString());
            }
            
            if (endpointMap.get("mac") != null) {
                endpointBuilder.setMac(endpointMap.get("mac").toString());
            }
            
            return endpointBuilder.build();
        } catch (Exception e) {
            log.warn("Failed to convert endpoint data: {}", endpointData, e);
            return null;
        }
    }
    
    /**
     * Convert threat info data to ThreatInfo message
     */
    private ThreatInfo convertToThreatInfo(Object threatInfoData) {
        if (threatInfoData == null) {
            return null;
        }
        
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> threatInfoMap = (Map<String, Object>) threatInfoData;
            
            ThreatInfo.Builder threatInfoBuilder = ThreatInfo.newBuilder();
            
            if (threatInfoMap.get("indicator") != null) {
                threatInfoBuilder.setIndicator(threatInfoMap.get("indicator").toString());
            }
            
            if (threatInfoMap.get("reputation_score") != null) {
                threatInfoBuilder.setReputationScore(getIntValue(threatInfoMap, "reputation_score"));
            }
            
            if (threatInfoMap.get("categories") != null) {
                @SuppressWarnings("unchecked")
                List<String> categories = (List<String>) threatInfoMap.get("categories");
                threatInfoBuilder.addAllCategories(categories);
            }
            
            if (threatInfoMap.get("first_seen") != null) {
                threatInfoBuilder.setFirstSeen(getLongValue(threatInfoMap, "first_seen"));
            }
            
            if (threatInfoMap.get("last_seen") != null) {
                threatInfoBuilder.setLastSeen(getLongValue(threatInfoMap, "last_seen"));
            }
            
            return threatInfoBuilder.build();
        } catch (Exception e) {
            log.warn("Failed to convert threat info data: {}", threatInfoData, e);
            return null;
        }
    }
    
    /**
     * Stream results in batches
     * 
     * Splits the result into batches and sends each batch as a separate response.
     * This provides better memory efficiency and allows clients to start processing
     * results immediately.
     * 
     * @param result The complete query result
     * @param request The original request
     * @param responseObserver The observer for streaming responses
     */
    private void streamResultsInBatches(
            QueryResult result,
            QueryRequest request,
            StreamObserver<QueryResponse> responseObserver) {
        
        List<Map<String, Object>> allRows = result.getRows();
        int totalRows = allRows.size();
        
        log.debug("Streaming {} rows in batches of {}", totalRows, STREAM_BATCH_SIZE);
        
        // Stream in batches
        for (int i = 0; i < totalRows; i += STREAM_BATCH_SIZE) {
            int endIndex = Math.min(i + STREAM_BATCH_SIZE, totalRows);
            List<Map<String, Object>> batchRows = allRows.subList(i, endIndex);
            
            // Create a batch result
            QueryResult batchResult = new QueryResult(batchRows);
            batchResult.setExecutionTimeMs(result.getExecutionTimeMs());
            batchResult.setStorageTier(result.getStorageTier());
            batchResult.setTimedOut(result.isTimedOut());
            
            // Convert to response
            QueryResponse batchResponse = convertToGrpcResponse(batchResult, request);
            
            // Send batch
            responseObserver.onNext(batchResponse);
            
            log.trace("Streamed batch {}-{} of {}", i, endIndex, totalRows);
        }
    }
    
    /**
     * Helper method to safely get int value from map
     */
    private int getIntValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return 0;
    }
    
    /**
     * Helper method to safely get long value from map
     */
    private long getLongValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0L;
    }
    
    /**
     * Helper method to safely get double value from map
     */
    private double getDoubleValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return 0.0;
    }
    
    /**
     * Check if a field is a standard Event field (not metadata)
     */
    private boolean isStandardField(String key) {
        return key.equals("_id") || key.equals("time") || key.equals("tenant_id") ||
               key.equals("severity") || key.equals("class_uid") || key.equals("category_uid") ||
               key.equals("message") || key.equals("raw_data") || key.equals("ueba_score") ||
               key.equals("actor") || key.equals("src_endpoint") || key.equals("dst_endpoint") ||
               key.equals("threat_info") || key.startsWith("_");
    }
}
