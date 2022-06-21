package com.aegis.query.grpc.interceptor;

import io.grpc.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * gRPC Logging Interceptor
 * 
 * This interceptor provides comprehensive logging for all gRPC requests and responses.
 * It logs request details, execution time, response status, and any errors that occur.
 * 
 * Features:
 * - Request logging (method name, metadata)
 * - Response logging (status, message count)
 * - Error logging with full stack traces
 * - Execution time tracking
 * - Metrics collection for monitoring
 * - Structured logging for easy parsing
 * 
 * The interceptor is applied globally to all gRPC services and runs after
 * the AuthenticationInterceptor (via @Order annotation).
 * 
 * Logging Levels:
 * - DEBUG: Detailed request/response information
 * - INFO: Request completion with timing
 * - WARN: Client errors (INVALID_ARGUMENT, NOT_FOUND, etc.)
 * - ERROR: Server errors (INTERNAL, UNAVAILABLE, etc.)
 * 
 * Metrics Tracked:
 * - Total requests by method
 * - Request duration histogram
 * - Status code distribution
 * - Error rates
 */
@Component
@GrpcGlobalServerInterceptor
@Order(2) // Run after AuthenticationInterceptor (which has default order 0)
public class LoggingInterceptor implements ServerInterceptor {
    
    private static final Logger log = LoggerFactory.getLogger(LoggingInterceptor.class);
    
    // Metrics
    private final MeterRegistry meterRegistry;
    private final Counter totalRequests;
    private final Timer requestDuration;
    
    /**
     * Constructor with dependency injection
     * 
     * @param meterRegistry Metrics registry for tracking request metrics
     */
    public LoggingInterceptor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        this.totalRequests = Counter.builder("grpc.requests.total")
            .description("Total gRPC requests")
            .register(meterRegistry);
        
        this.requestDuration = Timer.builder("grpc.requests.duration")
            .description("gRPC request duration")
            .register(meterRegistry);
        
        log.info("LoggingInterceptor initialized");
    }
    
    /**
     * Intercept gRPC calls to log request/response details
     * 
     * This method wraps the server call to:
     * 1. Log incoming request details
     * 2. Track execution time
     * 3. Log response status and details
     * 4. Log any errors that occur
     * 5. Collect metrics for monitoring
     * 
     * @param call The server call being intercepted
     * @param headers The request metadata/headers
     * @param next The next handler in the interceptor chain
     * @return A listener for the server call
     */
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        
        String methodName = call.getMethodDescriptor().getFullMethodName();
        String tenantId = AuthenticationInterceptor.getTenantIdFromContext();
        
        // Start timing
        long startTime = System.currentTimeMillis();
        Timer.Sample sample = Timer.start(meterRegistry);
        
        // Log request
        logRequest(methodName, tenantId, headers);
        
        // Increment request counter
        totalRequests.increment();
        Counter.builder("grpc.requests.by.method")
            .tag("method", methodName)
            .tag("tenant", tenantId != null ? tenantId : "unknown")
            .register(meterRegistry)
            .increment();
        
        // Wrap the call to intercept responses and errors
        ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
            
            private int messagesSent = 0;
            
            @Override
            public void sendMessage(RespT message) {
                messagesSent++;
                super.sendMessage(message);
            }
            
            @Override
            public void close(Status status, Metadata trailers) {
                long duration = System.currentTimeMillis() - startTime;
                sample.stop(requestDuration);
                
                // Log response
                logResponse(methodName, tenantId, status, messagesSent, duration);
                
                // Track status metrics
                Counter.builder("grpc.responses.by.status")
                    .tag("method", methodName)
                    .tag("status", status.getCode().name())
                    .tag("tenant", tenantId != null ? tenantId : "unknown")
                    .register(meterRegistry)
                    .increment();
                
                super.close(status, trailers);
            }
        };
        
        // Create listener wrapper to log messages and errors
        ServerCall.Listener<ReqT> listener = next.startCall(wrappedCall, headers);
        
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            
            private int messagesReceived = 0;
            
            @Override
            public void onMessage(ReqT message) {
                messagesReceived++;
                log.trace("Received message {} for {}", messagesReceived, methodName);
                super.onMessage(message);
            }
            
            @Override
            public void onHalfClose() {
                log.trace("Half close for {} after {} messages", methodName, messagesReceived);
                super.onHalfClose();
            }
            
            @Override
            public void onCancel() {
                long duration = System.currentTimeMillis() - startTime;
                log.warn("Request cancelled for {} after {}ms (tenant: {})", 
                    methodName, duration, tenantId);
                
                Counter.builder("grpc.requests.cancelled")
                    .tag("method", methodName)
                    .tag("tenant", tenantId != null ? tenantId : "unknown")
                    .register(meterRegistry)
                    .increment();
                
                super.onCancel();
            }
            
            @Override
            public void onComplete() {
                log.trace("Request completed for {} (tenant: {})", methodName, tenantId);
                super.onComplete();
            }
        };
    }
    
    /**
     * Log incoming request details
     * 
     * Logs at DEBUG level with:
     * - Method name
     * - Tenant ID (if authenticated)
     * - Selected metadata headers
     * 
     * @param methodName The gRPC method being called
     * @param tenantId The authenticated tenant ID
     * @param headers The request metadata
     */
    private void logRequest(String methodName, String tenantId, Metadata headers) {
        if (log.isDebugEnabled()) {
            StringBuilder logMessage = new StringBuilder();
            logMessage.append("gRPC Request: method=").append(methodName);
            
            if (tenantId != null) {
                logMessage.append(", tenant=").append(tenantId);
            }
            
            // Log selected headers (avoid logging sensitive data)
            String userAgent = headers.get(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER));
            if (userAgent != null) {
                logMessage.append(", user-agent=").append(userAgent);
            }
            
            String contentType = headers.get(Metadata.Key.of("content-type", Metadata.ASCII_STRING_MARSHALLER));
            if (contentType != null) {
                logMessage.append(", content-type=").append(contentType);
            }
            
            log.debug(logMessage.toString());
        }
    }
    
    /**
     * Log response details
     * 
     * Logs at different levels based on status:
     * - INFO: Successful responses (OK)
     * - WARN: Client errors (INVALID_ARGUMENT, NOT_FOUND, etc.)
     * - ERROR: Server errors (INTERNAL, UNAVAILABLE, etc.)
     * 
     * @param methodName The gRPC method that was called
     * @param tenantId The authenticated tenant ID
     * @param status The response status
     * @param messagesSent Number of response messages sent
     * @param durationMs Request duration in milliseconds
     */
    private void logResponse(String methodName, String tenantId, Status status, 
                            int messagesSent, long durationMs) {
        
        String logMessage = String.format(
            "gRPC Response: method=%s, tenant=%s, status=%s, messages=%d, duration=%dms",
            methodName,
            tenantId != null ? tenantId : "unknown",
            status.getCode(),
            messagesSent,
            durationMs
        );
        
        if (status.getDescription() != null && !status.getDescription().isEmpty()) {
            logMessage += ", description=" + status.getDescription();
        }
        
        // Log at appropriate level based on status
        if (status.isOk()) {
            log.info(logMessage);
        } else if (isClientError(status.getCode())) {
            log.warn(logMessage);
        } else {
            log.error(logMessage, status.getCause());
        }
    }
    
    /**
     * Determine if a status code represents a client error
     * 
     * Client errors are typically caused by invalid requests and should
     * be logged at WARN level rather than ERROR.
     * 
     * @param code The status code
     * @return true if this is a client error
     */
    private boolean isClientError(Status.Code code) {
        return code == Status.Code.INVALID_ARGUMENT ||
               code == Status.Code.NOT_FOUND ||
               code == Status.Code.ALREADY_EXISTS ||
               code == Status.Code.PERMISSION_DENIED ||
               code == Status.Code.UNAUTHENTICATED ||
               code == Status.Code.FAILED_PRECONDITION ||
               code == Status.Code.OUT_OF_RANGE ||
               code == Status.Code.UNIMPLEMENTED;
    }
}
