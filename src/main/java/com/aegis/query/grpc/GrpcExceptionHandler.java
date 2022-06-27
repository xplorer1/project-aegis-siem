package com.aegis.query.grpc;

import com.aegis.normalization.parsers.ParseException;
import com.aegis.query.QueryExecutionException;
import com.aegis.security.TenantAccessDeniedException;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.DebugInfo;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Help;
import com.google.rpc.QuotaFailure;
import com.google.rpc.ResourceInfo;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.protobuf.StatusProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeoutException;

/**
 * GrpcExceptionHandler provides centralized exception-to-Status mapping for gRPC services.
 * 
 * This handler:
 * - Maps Java exceptions to appropriate gRPC Status codes
 * - Adds rich error details using google.rpc.Status
 * - Provides structured error information for clients
 * - Sanitizes error messages to prevent information leakage
 * - Includes debug information for development/troubleshooting
 * 
 * Status Code Mapping:
 * - IllegalArgumentException -> INVALID_ARGUMENT (validation errors)
 * - ParseException -> INVALID_ARGUMENT (query parsing errors)
 * - QueryExecutionException -> INTERNAL (query execution failures)
 * - TimeoutException -> DEADLINE_EXCEEDED (query timeouts)
 * - SecurityException -> PERMISSION_DENIED (authorization failures)
 * - UnsupportedOperationException -> UNIMPLEMENTED (unsupported features)
 * - IllegalStateException -> FAILED_PRECONDITION (invalid state)
 * - NullPointerException -> INTERNAL (programming errors)
 * - RuntimeException -> INTERNAL (general errors)
 * 
 * Error Details:
 * The handler enriches errors with google.rpc error details:
 * - ErrorInfo: Machine-readable error reason and domain
 * - BadRequest: Field-level validation errors
 * - DebugInfo: Stack traces and debug information
 * - ResourceInfo: Information about affected resources
 * - Help: Links to documentation or support
 * 
 * Usage:
 * <pre>
 * try {
 *     // Execute query
 * } catch (Exception e) {
 *     throw exceptionHandler.handleException(e, "executeQuery", tenantId);
 * }
 * </pre>
 */
@Component
public class GrpcExceptionHandler {
    
    private static final Logger log = LoggerFactory.getLogger(GrpcExceptionHandler.class);
    
    // Error domains for ErrorInfo
    private static final String ERROR_DOMAIN = "aegis.siem.query";
    
    // Error reasons
    private static final String REASON_VALIDATION_FAILED = "VALIDATION_FAILED";
    private static final String REASON_QUERY_PARSE_FAILED = "QUERY_PARSE_FAILED";
    private static final String REASON_QUERY_EXECUTION_FAILED = "QUERY_EXECUTION_FAILED";
    private static final String REASON_QUERY_TIMEOUT = "QUERY_TIMEOUT";
    private static final String REASON_PERMISSION_DENIED = "PERMISSION_DENIED";
    private static final String REASON_TENANT_ACCESS_DENIED = "TENANT_ACCESS_DENIED";
    private static final String REASON_UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION";
    private static final String REASON_INVALID_STATE = "INVALID_STATE";
    private static final String REASON_INTERNAL_ERROR = "INTERNAL_ERROR";
    
    // Help URLs
    private static final String HELP_URL_BASE = "https://docs.aegis-siem.io/errors/";
    
    /**
     * Handle an exception and convert it to a StatusRuntimeException with rich error details
     * 
     * @param exception The exception to handle
     * @param method The gRPC method name where the error occurred
     * @param tenantId The tenant ID (for context)
     * @return A StatusRuntimeException with appropriate status code and error details
     */
    public StatusRuntimeException handleException(Throwable exception, String method, String tenantId) {
        log.debug("Handling exception in method {} for tenant {}: {}", 
            method, tenantId, exception.getClass().getSimpleName());
        
        // Map exception to Status
        Status status = mapExceptionToStatus(exception);
        
        // Build com.google.rpc.Status with error details
        com.google.rpc.Status rpcStatus = buildRpcStatus(status, exception, method, tenantId);
        
        // Convert to StatusRuntimeException
        StatusRuntimeException statusException = StatusProto.toStatusRuntimeException(rpcStatus);
        
        // Log the error with appropriate level
        logException(exception, status, method, tenantId);
        
        return statusException;
    }
    
    /**
     * Map a Java exception to a gRPC Status code
     * 
     * @param exception The exception to map
     * @return The appropriate gRPC Status
     */
    private Status mapExceptionToStatus(Throwable exception) {
        // Tenant access denied errors (Requirement 9.6)
        if (exception instanceof TenantAccessDeniedException) {
            TenantAccessDeniedException tenantEx = (TenantAccessDeniedException) exception;
            String message = "Tenant access denied";
            if (tenantEx.getAuthenticatedTenantId() != null && tenantEx.getRequestedTenantId() != null) {
                message = String.format("Tenant '%s' cannot access resources for tenant '%s'",
                    tenantEx.getAuthenticatedTenantId(), tenantEx.getRequestedTenantId());
            }
            return Status.PERMISSION_DENIED
                .withDescription(message)
                .withCause(exception);
        }
        
        // Validation errors
        if (exception instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT
                .withDescription(sanitizeMessage(exception.getMessage()))
                .withCause(exception);
        }
        
        // Query parsing errors
        if (exception instanceof ParseException) {
            ParseException parseEx = (ParseException) exception;
            String message = String.format("Query parsing failed: %s", 
                sanitizeMessage(parseEx.getMessage()));
            return Status.INVALID_ARGUMENT
                .withDescription(message)
                .withCause(exception);
        }
        
        // Query execution errors
        if (exception instanceof QueryExecutionException) {
            QueryExecutionException queryEx = (QueryExecutionException) exception;
            String message = String.format("Query execution failed on tier %s: %s",
                queryEx.getTier(), sanitizeMessage(queryEx.getMessage()));
            return Status.INTERNAL
                .withDescription(message)
                .withCause(exception);
        }
        
        // Timeout errors
        if (exception instanceof TimeoutException) {
            return Status.DEADLINE_EXCEEDED
                .withDescription("Query execution timed out")
                .withCause(exception);
        }
        
        // Permission errors
        if (exception instanceof SecurityException) {
            return Status.PERMISSION_DENIED
                .withDescription("Permission denied")
                .withCause(exception);
        }
        
        // Unsupported operations
        if (exception instanceof UnsupportedOperationException) {
            return Status.UNIMPLEMENTED
                .withDescription(sanitizeMessage(exception.getMessage()))
                .withCause(exception);
        }
        
        // Invalid state errors
        if (exception instanceof IllegalStateException) {
            return Status.FAILED_PRECONDITION
                .withDescription(sanitizeMessage(exception.getMessage()))
                .withCause(exception);
        }
        
        // Null pointer errors (programming errors)
        if (exception instanceof NullPointerException) {
            return Status.INTERNAL
                .withDescription("Internal error: null pointer exception")
                .withCause(exception);
        }
        
        // Default: internal error
        return Status.INTERNAL
            .withDescription("Internal server error")
            .withCause(exception);
    }
    
    /**
     * Build a com.google.rpc.Status with rich error details
     * 
     * @param status The gRPC Status
     * @param exception The original exception
     * @param method The method name
     * @param tenantId The tenant ID
     * @return A com.google.rpc.Status with error details
     */
    private com.google.rpc.Status buildRpcStatus(
            Status status, 
            Throwable exception, 
            String method, 
            String tenantId) {
        
        com.google.rpc.Status.Builder statusBuilder = com.google.rpc.Status.newBuilder()
            .setCode(status.getCode().value())
            .setMessage(status.getDescription() != null ? status.getDescription() : "");
        
        // Add ErrorInfo with reason and metadata
        ErrorInfo errorInfo = buildErrorInfo(exception, method, tenantId);
        statusBuilder.addDetails(Any.pack(errorInfo));
        
        // Add BadRequest for validation errors
        if (exception instanceof IllegalArgumentException) {
            BadRequest badRequest = buildBadRequest(exception);
            statusBuilder.addDetails(Any.pack(badRequest));
        }
        
        // Add DebugInfo with stack trace (only in development)
        if (shouldIncludeDebugInfo()) {
            DebugInfo debugInfo = buildDebugInfo(exception);
            statusBuilder.addDetails(Any.pack(debugInfo));
        }
        
        // Add ResourceInfo for query execution errors
        if (exception instanceof QueryExecutionException) {
            ResourceInfo resourceInfo = buildResourceInfo((QueryExecutionException) exception);
            statusBuilder.addDetails(Any.pack(resourceInfo));
        }
        
        // Add Help with documentation links
        Help help = buildHelp(exception);
        statusBuilder.addDetails(Any.pack(help));
        
        return statusBuilder.build();
    }
    
    /**
     * Build ErrorInfo with error reason and metadata
     * 
     * @param exception The exception
     * @param method The method name
     * @param tenantId The tenant ID
     * @return An ErrorInfo message
     */
    private ErrorInfo buildErrorInfo(Throwable exception, String method, String tenantId) {
        ErrorInfo.Builder builder = ErrorInfo.newBuilder()
            .setDomain(ERROR_DOMAIN)
            .setReason(getErrorReason(exception));
        
        // Add metadata
        builder.putMetadata("method", method);
        if (tenantId != null) {
            builder.putMetadata("tenant_id", tenantId);
        }
        builder.putMetadata("exception_type", exception.getClass().getSimpleName());
        
        // Add tier information for QueryExecutionException
        if (exception instanceof QueryExecutionException) {
            QueryExecutionException queryEx = (QueryExecutionException) exception;
            if (queryEx.getTier() != null) {
                builder.putMetadata("storage_tier", queryEx.getTier().name());
            }
        }
        
        // Add vendor type for ParseException
        if (exception instanceof ParseException) {
            ParseException parseEx = (ParseException) exception;
            if (parseEx.getVendorType() != null) {
                builder.putMetadata("vendor_type", parseEx.getVendorType());
            }
        }
        
        // Add tenant information for TenantAccessDeniedException
        if (exception instanceof TenantAccessDeniedException) {
            TenantAccessDeniedException tenantEx = (TenantAccessDeniedException) exception;
            if (tenantEx.getAuthenticatedTenantId() != null) {
                builder.putMetadata("authenticated_tenant_id", tenantEx.getAuthenticatedTenantId());
            }
            if (tenantEx.getRequestedTenantId() != null) {
                builder.putMetadata("requested_tenant_id", tenantEx.getRequestedTenantId());
            }
        }
        
        return builder.build();
    }
    
    /**
     * Build BadRequest with field violations
     * 
     * @param exception The exception
     * @return A BadRequest message
     */
    private BadRequest buildBadRequest(Throwable exception) {
        BadRequest.Builder builder = BadRequest.newBuilder();
        
        // Parse the exception message to extract field violations
        String message = exception.getMessage();
        if (message != null) {
            // Simple heuristic: if message contains "field" or specific field names
            if (message.toLowerCase().contains("tenant") || message.toLowerCase().contains("tenant_id")) {
                builder.addFieldViolations(BadRequest.FieldViolation.newBuilder()
                    .setField("tenant_id")
                    .setDescription(message)
                    .build());
            } else if (message.toLowerCase().contains("query")) {
                builder.addFieldViolations(BadRequest.FieldViolation.newBuilder()
                    .setField("query")
                    .setDescription(message)
                    .build());
            } else if (message.toLowerCase().contains("limit")) {
                builder.addFieldViolations(BadRequest.FieldViolation.newBuilder()
                    .setField("limit")
                    .setDescription(message)
                    .build());
            } else if (message.toLowerCase().contains("offset")) {
                builder.addFieldViolations(BadRequest.FieldViolation.newBuilder()
                    .setField("offset")
                    .setDescription(message)
                    .build());
            } else if (message.toLowerCase().contains("timeout")) {
                builder.addFieldViolations(BadRequest.FieldViolation.newBuilder()
                    .setField("timeout_seconds")
                    .setDescription(message)
                    .build());
            } else {
                // Generic violation
                builder.addFieldViolations(BadRequest.FieldViolation.newBuilder()
                    .setField("request")
                    .setDescription(message)
                    .build());
            }
        }
        
        return builder.build();
    }
    
    /**
     * Build DebugInfo with stack trace
     * 
     * @param exception The exception
     * @return A DebugInfo message
     */
    private DebugInfo buildDebugInfo(Throwable exception) {
        DebugInfo.Builder builder = DebugInfo.newBuilder();
        
        // Add exception message
        if (exception.getMessage() != null) {
            builder.setDetail(exception.getMessage());
        }
        
        // Add stack trace entries (limit to first 10 for brevity)
        StackTraceElement[] stackTrace = exception.getStackTrace();
        int limit = Math.min(stackTrace.length, 10);
        for (int i = 0; i < limit; i++) {
            builder.addStackEntries(stackTrace[i].toString());
        }
        
        // Add cause information if present
        if (exception.getCause() != null) {
            builder.setDetail(builder.getDetail() + " | Caused by: " + exception.getCause().getMessage());
        }
        
        return builder.build();
    }
    
    /**
     * Build ResourceInfo for query execution errors
     * 
     * @param exception The QueryExecutionException
     * @return A ResourceInfo message
     */
    private ResourceInfo buildResourceInfo(QueryExecutionException exception) {
        ResourceInfo.Builder builder = ResourceInfo.newBuilder()
            .setResourceType("query")
            .setResourceName(exception.getQuery() != null ? exception.getQuery() : "unknown");
        
        if (exception.getTier() != null) {
            builder.setDescription("Query failed on storage tier: " + exception.getTier().name());
        }
        
        return builder.build();
    }
    
    /**
     * Build Help with documentation links
     * 
     * @param exception The exception
     * @return A Help message
     */
    private Help buildHelp(Throwable exception) {
        Help.Builder builder = Help.newBuilder();
        
        // Add general documentation link
        builder.addLinks(Help.Link.newBuilder()
            .setDescription("AEGIS Query API Documentation")
            .setUrl(HELP_URL_BASE + "query-api")
            .build());
        
        // Add specific error documentation based on exception type
        if (exception instanceof IllegalArgumentException) {
            builder.addLinks(Help.Link.newBuilder()
                .setDescription("Query Validation Guide")
                .setUrl(HELP_URL_BASE + "validation")
                .build());
        } else if (exception instanceof ParseException) {
            builder.addLinks(Help.Link.newBuilder()
                .setDescription("AQL Syntax Reference")
                .setUrl(HELP_URL_BASE + "aql-syntax")
                .build());
        } else if (exception instanceof QueryExecutionException) {
            builder.addLinks(Help.Link.newBuilder()
                .setDescription("Query Troubleshooting Guide")
                .setUrl(HELP_URL_BASE + "troubleshooting")
                .build());
        } else if (exception instanceof TimeoutException) {
            builder.addLinks(Help.Link.newBuilder()
                .setDescription("Query Performance Optimization")
                .setUrl(HELP_URL_BASE + "performance")
                .build());
        }
        
        // Add support contact
        builder.addLinks(Help.Link.newBuilder()
            .setDescription("Contact Support")
            .setUrl("https://support.aegis-siem.io")
            .build());
        
        return builder.build();
    }
    
    /**
     * Get the error reason string for an exception
     * 
     * @param exception The exception
     * @return The error reason
     */
    private String getErrorReason(Throwable exception) {
        if (exception instanceof TenantAccessDeniedException) {
            return REASON_TENANT_ACCESS_DENIED;
        } else if (exception instanceof IllegalArgumentException) {
            return REASON_VALIDATION_FAILED;
        } else if (exception instanceof ParseException) {
            return REASON_QUERY_PARSE_FAILED;
        } else if (exception instanceof QueryExecutionException) {
            return REASON_QUERY_EXECUTION_FAILED;
        } else if (exception instanceof TimeoutException) {
            return REASON_QUERY_TIMEOUT;
        } else if (exception instanceof SecurityException) {
            return REASON_PERMISSION_DENIED;
        } else if (exception instanceof UnsupportedOperationException) {
            return REASON_UNSUPPORTED_OPERATION;
        } else if (exception instanceof IllegalStateException) {
            return REASON_INVALID_STATE;
        } else {
            return REASON_INTERNAL_ERROR;
        }
    }
    
    /**
     * Sanitize an error message to prevent information leakage
     * 
     * Removes sensitive information like:
     * - File paths
     * - Database connection strings
     * - Internal implementation details
     * 
     * @param message The original message
     * @return The sanitized message
     */
    private String sanitizeMessage(String message) {
        if (message == null) {
            return "An error occurred";
        }
        
        // Remove file paths (both Unix and Windows)
        message = message.replaceAll("/[\\w/.-]+", "[path]");
        message = message.replaceAll("[A-Z]:\\\\[\\w\\\\.-]+", "[path]");
        
        // Remove connection strings
        message = message.replaceAll("jdbc:[^\\s]+", "[connection-string]");
        message = message.replaceAll("https?://[^\\s]+", "[url]");
        
        // Remove IP addresses
        message = message.replaceAll("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b", "[ip-address]");
        
        // Truncate very long messages
        if (message.length() > 500) {
            message = message.substring(0, 497) + "...";
        }
        
        return message;
    }
    
    /**
     * Determine if debug information should be included
     * 
     * Debug info includes stack traces and should only be enabled in development.
     * In production, this should be false to avoid leaking implementation details.
     * 
     * @return true if debug info should be included
     */
    private boolean shouldIncludeDebugInfo() {
        // Check environment variable or system property
        String env = System.getProperty("aegis.environment", "production");
        return "development".equalsIgnoreCase(env) || "dev".equalsIgnoreCase(env);
    }
    
    /**
     * Log the exception with appropriate level based on status
     * 
     * @param exception The exception
     * @param status The gRPC status
     * @param method The method name
     * @param tenantId The tenant ID
     */
    private void logException(Throwable exception, Status status, String method, String tenantId) {
        String logMessage = String.format("gRPC error in %s for tenant %s: %s - %s",
            method, tenantId, status.getCode(), status.getDescription());
        
        // Log at different levels based on status code
        switch (status.getCode()) {
            case INVALID_ARGUMENT:
            case FAILED_PRECONDITION:
            case UNIMPLEMENTED:
                // Client errors - log at WARN level
                log.warn(logMessage, exception);
                break;
                
            case DEADLINE_EXCEEDED:
                // Timeout - log at WARN level
                log.warn(logMessage);
                break;
                
            case PERMISSION_DENIED:
                // Security - log at WARN level with details
                log.warn(logMessage, exception);
                break;
                
            case INTERNAL:
            case UNKNOWN:
            default:
                // Server errors - log at ERROR level
                log.error(logMessage, exception);
                break;
        }
    }
    
    /**
     * Create a simple StatusRuntimeException for common cases
     * 
     * This is a convenience method for creating simple errors without rich details.
     * 
     * @param status The gRPC Status
     * @param message The error message
     * @return A StatusRuntimeException
     */
    public StatusRuntimeException createSimpleException(Status status, String message) {
        return status.withDescription(sanitizeMessage(message)).asRuntimeException();
    }
    
    /**
     * Create a StatusRuntimeException with metadata
     * 
     * @param status The gRPC Status
     * @param message The error message
     * @param metadata Additional metadata to include
     * @return A StatusRuntimeException with metadata
     */
    public StatusRuntimeException createExceptionWithMetadata(
            Status status, 
            String message, 
            Metadata metadata) {
        return status.withDescription(sanitizeMessage(message)).asRuntimeException(metadata);
    }
}
