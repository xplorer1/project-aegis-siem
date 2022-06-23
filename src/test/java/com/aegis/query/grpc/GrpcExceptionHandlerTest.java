package com.aegis.query.grpc;

import com.aegis.domain.StorageTier;
import com.aegis.normalization.parsers.ParseException;
import com.aegis.query.QueryExecutionException;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.DebugInfo;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Help;
import com.google.rpc.ResourceInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for GrpcExceptionHandler
 * 
 * Tests exception-to-Status mapping and error detail generation
 */
class GrpcExceptionHandlerTest {
    
    private GrpcExceptionHandler handler;
    
    @BeforeEach
    void setUp() {
        handler = new GrpcExceptionHandler();
    }
    
    @Test
    void testHandleIllegalArgumentException() {
        // Given
        IllegalArgumentException exception = new IllegalArgumentException("Tenant ID is required");
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(result.getStatus().getDescription()).contains("Tenant ID is required");
        
        // Verify error details
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        assertThat(rpcStatus).isNotNull();
        assertThat(rpcStatus.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT.value());
        
        // Check for ErrorInfo
        boolean hasErrorInfo = rpcStatus.getDetailsList().stream()
            .anyMatch(any -> any.is(ErrorInfo.class));
        assertThat(hasErrorInfo).isTrue();
        
        // Check for BadRequest
        boolean hasBadRequest = rpcStatus.getDetailsList().stream()
            .anyMatch(any -> any.is(BadRequest.class));
        assertThat(hasBadRequest).isTrue();
        
        // Check for Help
        boolean hasHelp = rpcStatus.getDetailsList().stream()
            .anyMatch(any -> any.is(Help.class));
        assertThat(hasHelp).isTrue();
    }
    
    @Test
    void testHandleParseException() {
        // Given
        ParseException exception = new ParseException(
            "Invalid AQL syntax", 
            "cloudtrail", 
            "source=events | where invalid"
        );
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(result.getStatus().getDescription()).contains("Query parsing failed");
        
        // Verify ErrorInfo contains vendor type
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        ErrorInfo errorInfo = extractErrorInfo(rpcStatus);
        assertThat(errorInfo).isNotNull();
        assertThat(errorInfo.getReason()).isEqualTo("QUERY_PARSE_FAILED");
        assertThat(errorInfo.getMetadataMap()).containsEntry("vendor_type", "cloudtrail");
        assertThat(errorInfo.getMetadataMap()).containsEntry("method", method);
        assertThat(errorInfo.getMetadataMap()).containsEntry("tenant_id", tenantId);
    }
    
    @Test
    void testHandleQueryExecutionException() {
        // Given
        QueryExecutionException exception = new QueryExecutionException(
            "OpenSearch query failed",
            StorageTier.HOT,
            "source=events | stats count() by severity",
            new RuntimeException("Connection timeout")
        );
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(result.getStatus().getDescription()).contains("Query execution failed");
        assertThat(result.getStatus().getDescription()).contains("HOT");
        
        // Verify ErrorInfo contains tier
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        ErrorInfo errorInfo = extractErrorInfo(rpcStatus);
        assertThat(errorInfo).isNotNull();
        assertThat(errorInfo.getReason()).isEqualTo("QUERY_EXECUTION_FAILED");
        assertThat(errorInfo.getMetadataMap()).containsEntry("storage_tier", "HOT");
        
        // Check for ResourceInfo
        ResourceInfo resourceInfo = extractResourceInfo(rpcStatus);
        assertThat(resourceInfo).isNotNull();
        assertThat(resourceInfo.getResourceType()).isEqualTo("query");
        assertThat(resourceInfo.getDescription()).contains("HOT");
    }
    
    @Test
    void testHandleTimeoutException() {
        // Given
        TimeoutException exception = new TimeoutException("Query execution timed out");
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
        assertThat(result.getStatus().getDescription()).contains("timed out");
        
        // Verify ErrorInfo
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        ErrorInfo errorInfo = extractErrorInfo(rpcStatus);
        assertThat(errorInfo).isNotNull();
        assertThat(errorInfo.getReason()).isEqualTo("QUERY_TIMEOUT");
    }
    
    @Test
    void testHandleSecurityException() {
        // Given
        SecurityException exception = new SecurityException("Access denied");
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(result.getStatus().getDescription()).contains("Permission denied");
        
        // Verify ErrorInfo
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        ErrorInfo errorInfo = extractErrorInfo(rpcStatus);
        assertThat(errorInfo).isNotNull();
        assertThat(errorInfo.getReason()).isEqualTo("PERMISSION_DENIED");
    }
    
    @Test
    void testHandleUnsupportedOperationException() {
        // Given
        UnsupportedOperationException exception = new UnsupportedOperationException(
            "Cold tier queries not yet implemented"
        );
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
        assertThat(result.getStatus().getDescription()).contains("not yet implemented");
        
        // Verify ErrorInfo
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        ErrorInfo errorInfo = extractErrorInfo(rpcStatus);
        assertThat(errorInfo).isNotNull();
        assertThat(errorInfo.getReason()).isEqualTo("UNSUPPORTED_OPERATION");
    }
    
    @Test
    void testHandleIllegalStateException() {
        // Given
        IllegalStateException exception = new IllegalStateException("Query plan is empty");
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(result.getStatus().getDescription()).contains("Query plan is empty");
        
        // Verify ErrorInfo
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        ErrorInfo errorInfo = extractErrorInfo(rpcStatus);
        assertThat(errorInfo).isNotNull();
        assertThat(errorInfo.getReason()).isEqualTo("INVALID_STATE");
    }
    
    @Test
    void testHandleNullPointerException() {
        // Given
        NullPointerException exception = new NullPointerException("Query executor is null");
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(result.getStatus().getDescription()).contains("null pointer");
        
        // Verify ErrorInfo
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        ErrorInfo errorInfo = extractErrorInfo(rpcStatus);
        assertThat(errorInfo).isNotNull();
        assertThat(errorInfo.getReason()).isEqualTo("INTERNAL_ERROR");
    }
    
    @Test
    void testHandleGenericRuntimeException() {
        // Given
        RuntimeException exception = new RuntimeException("Unexpected error");
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(result.getStatus().getDescription()).contains("Internal server error");
        
        // Verify ErrorInfo
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        ErrorInfo errorInfo = extractErrorInfo(rpcStatus);
        assertThat(errorInfo).isNotNull();
        assertThat(errorInfo.getReason()).isEqualTo("INTERNAL_ERROR");
    }
    
    @Test
    void testBadRequestFieldViolations() {
        // Given
        IllegalArgumentException exception = new IllegalArgumentException("Limit exceeds maximum allowed value");
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        BadRequest badRequest = extractBadRequest(rpcStatus);
        assertThat(badRequest).isNotNull();
        assertThat(badRequest.getFieldViolationsList()).isNotEmpty();
        
        // Should have a field violation for "limit"
        boolean hasLimitViolation = badRequest.getFieldViolationsList().stream()
            .anyMatch(v -> v.getField().equals("limit"));
        assertThat(hasLimitViolation).isTrue();
    }
    
    @Test
    void testHelpLinksIncluded() {
        // Given
        IllegalArgumentException exception = new IllegalArgumentException("Invalid query");
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        Help help = extractHelp(rpcStatus);
        assertThat(help).isNotNull();
        assertThat(help.getLinksList()).isNotEmpty();
        
        // Should have at least documentation and support links
        assertThat(help.getLinksList().size()).isGreaterThanOrEqualTo(2);
        
        // Check for support link
        boolean hasSupportLink = help.getLinksList().stream()
            .anyMatch(link -> link.getUrl().contains("support"));
        assertThat(hasSupportLink).isTrue();
    }
    
    @Test
    void testMessageSanitization() {
        // Given - exception with sensitive information
        IllegalArgumentException exception = new IllegalArgumentException(
            "Failed to connect to jdbc:clickhouse://192.168.1.100:8123/aegis at /home/user/config.xml"
        );
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then - sensitive info should be sanitized
        String description = result.getStatus().getDescription();
        assertThat(description).doesNotContain("jdbc:clickhouse://");
        assertThat(description).doesNotContain("192.168.1.100");
        assertThat(description).doesNotContain("/home/user/config.xml");
        assertThat(description).contains("[connection-string]");
        assertThat(description).contains("[ip-address]");
        assertThat(description).contains("[path]");
    }
    
    @Test
    void testCreateSimpleException() {
        // Given
        Status status = Status.INVALID_ARGUMENT;
        String message = "Invalid request";
        
        // When
        StatusRuntimeException result = handler.createSimpleException(status, message);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(result.getStatus().getDescription()).isEqualTo(message);
    }
    
    @Test
    void testErrorInfoMetadata() {
        // Given
        QueryExecutionException exception = new QueryExecutionException(
            "Query failed",
            StorageTier.WARM,
            "source=events | stats count()",
            new RuntimeException("Database error")
        );
        String method = "executeQuery";
        String tenantId = "test-tenant-123";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        ErrorInfo errorInfo = extractErrorInfo(rpcStatus);
        assertThat(errorInfo).isNotNull();
        
        // Verify all metadata fields
        assertThat(errorInfo.getDomain()).isEqualTo("aegis.siem.query");
        assertThat(errorInfo.getReason()).isEqualTo("QUERY_EXECUTION_FAILED");
        assertThat(errorInfo.getMetadataMap()).containsEntry("method", "executeQuery");
        assertThat(errorInfo.getMetadataMap()).containsEntry("tenant_id", "test-tenant-123");
        assertThat(errorInfo.getMetadataMap()).containsEntry("storage_tier", "WARM");
        assertThat(errorInfo.getMetadataMap()).containsEntry("exception_type", "QueryExecutionException");
    }
    
    @Test
    void testNullTenantIdHandled() {
        // Given
        IllegalArgumentException exception = new IllegalArgumentException("Invalid query");
        String method = "executeQuery";
        String tenantId = null;
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then - should not throw NPE
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        
        // ErrorInfo should not have tenant_id in metadata
        com.google.rpc.Status rpcStatus = StatusProto.fromThrowable(result);
        ErrorInfo errorInfo = extractErrorInfo(rpcStatus);
        assertThat(errorInfo).isNotNull();
        assertThat(errorInfo.getMetadataMap()).doesNotContainKey("tenant_id");
    }
    
    @Test
    void testNullExceptionMessageHandled() {
        // Given - exception with null message
        IllegalArgumentException exception = new IllegalArgumentException((String) null);
        String method = "executeQuery";
        String tenantId = "test-tenant";
        
        // When
        StatusRuntimeException result = handler.handleException(exception, method, tenantId);
        
        // Then - should not throw NPE and should have a default message
        assertThat(result).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(result.getStatus().getDescription()).isNotNull();
    }
    
    // Helper methods to extract error details from com.google.rpc.Status
    
    private ErrorInfo extractErrorInfo(com.google.rpc.Status rpcStatus) {
        return rpcStatus.getDetailsList().stream()
            .filter(any -> any.is(ErrorInfo.class))
            .map(any -> {
                try {
                    return any.unpack(ErrorInfo.class);
                } catch (Exception e) {
                    return null;
                }
            })
            .filter(info -> info != null)
            .findFirst()
            .orElse(null);
    }
    
    private BadRequest extractBadRequest(com.google.rpc.Status rpcStatus) {
        return rpcStatus.getDetailsList().stream()
            .filter(any -> any.is(BadRequest.class))
            .map(any -> {
                try {
                    return any.unpack(BadRequest.class);
                } catch (Exception e) {
                    return null;
                }
            })
            .filter(req -> req != null)
            .findFirst()
            .orElse(null);
    }
    
    private ResourceInfo extractResourceInfo(com.google.rpc.Status rpcStatus) {
        return rpcStatus.getDetailsList().stream()
            .filter(any -> any.is(ResourceInfo.class))
            .map(any -> {
                try {
                    return any.unpack(ResourceInfo.class);
                } catch (Exception e) {
                    return null;
                }
            })
            .filter(info -> info != null)
            .findFirst()
            .orElse(null);
    }
    
    private Help extractHelp(com.google.rpc.Status rpcStatus) {
        return rpcStatus.getDetailsList().stream()
            .filter(any -> any.is(Help.class))
            .map(any -> {
                try {
                    return any.unpack(Help.class);
                } catch (Exception e) {
                    return null;
                }
            })
            .filter(help -> help != null)
            .findFirst()
            .orElse(null);
    }
}
