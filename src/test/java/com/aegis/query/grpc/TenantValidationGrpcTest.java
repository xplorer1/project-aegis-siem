package com.aegis.query.grpc;

import com.aegis.domain.QueryResult;
import com.aegis.domain.StorageTier;
import com.aegis.query.AqlTranspiler;
import com.aegis.query.QueryExecutor;
import com.aegis.query.QueryPlan;
import com.aegis.query.grpc.proto.*;
import com.aegis.security.TenantAccessDeniedException;
import com.aegis.security.TenantContext;
import com.aegis.security.TenantValidator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration tests for tenant validation in gRPC Query Service
 * 
 * Tests Requirement 9.6: "THE API_Gateway SHALL validate tenant_id in 
 * authentication tokens matches tenant_id in all API requests"
 * 
 * These tests verify that:
 * 1. Authenticated tenants can only query their own data
 * 2. Cross-tenant query attempts are rejected with PERMISSION_DENIED
 * 3. Proper error details are included in gRPC Status
 */
@DisplayName("gRPC Tenant Validation Integration Tests")
class TenantValidationGrpcTest {
    
    private QueryGrpcService queryGrpcService;
    private QueryExecutor queryExecutor;
    private AqlTranspiler aqlTranspiler;
    private GrpcExceptionHandler exceptionHandler;
    private TenantValidator tenantValidator;
    private MeterRegistry meterRegistry;
    
    private static final String TENANT_1 = "tenant-123";
    private static final String TENANT_2 = "tenant-456";
    
    @BeforeEach
    void setUp() {
        queryExecutor = mock(QueryExecutor.class);
        aqlTranspiler = mock(AqlTranspiler.class);
        meterRegistry = new SimpleMeterRegistry();
        tenantValidator = new TenantValidator();
        exceptionHandler = new GrpcExceptionHandler();
        
        queryGrpcService = new QueryGrpcService(
            queryExecutor,
            aqlTranspiler,
            meterRegistry,
            exceptionHandler,
            tenantValidator
        );
        
        TenantContext.clear();
    }
    
    @AfterEach
    void tearDown() {
        TenantContext.clear();
    }
    
    @Test
    @DisplayName("executeQuery should succeed when tenant IDs match")
    void executeQueryShouldSucceedWhenTenantIdsMatch() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        QueryRequest request = QueryRequest.newBuilder()
            .setTenantId(TENANT_1)
            .setQuery("source=firewall | where severity > 3")
            .setLimit(100)
            .build();
        
        QueryPlan queryPlan = new QueryPlan();
        QueryResult queryResult = createQueryResult(TENANT_1, 10);
        
        when(aqlTranspiler.transpile(anyString())).thenReturn(queryPlan);
        when(queryExecutor.execute(any(QueryPlan.class))).thenReturn(Flux.just(queryResult));
        
        TestStreamObserver<QueryResponse> responseObserver = new TestStreamObserver<>();
        
        // When
        queryGrpcService.executeQuery(request, responseObserver);
        
        // Then
        assertThat(responseObserver.isCompleted()).isTrue();
        assertThat(responseObserver.getError()).isNull();
        assertThat(responseObserver.getValues()).hasSize(1);
        
        QueryResponse response = responseObserver.getValues().get(0);
        assertThat(response.getCode()).isEqualTo(0); // Success
        assertThat(response.getTotalCount()).isEqualTo(10);
        
        verify(aqlTranspiler).transpile("source=firewall | where severity > 3");
        verify(queryExecutor).execute(queryPlan);
    }
    
    @Test
    @DisplayName("executeQuery should reject when tenant IDs do not match")
    void executeQueryShouldRejectWhenTenantIdsDoNotMatch() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        QueryRequest request = QueryRequest.newBuilder()
            .setTenantId(TENANT_2) // Different tenant
            .setQuery("source=firewall | where severity > 3")
            .setLimit(100)
            .build();
        
        TestStreamObserver<QueryResponse> responseObserver = new TestStreamObserver<>();
        
        // When
        queryGrpcService.executeQuery(request, responseObserver);
        
        // Then
        assertThat(responseObserver.isCompleted()).isFalse();
        assertThat(responseObserver.getError()).isNotNull();
        
        StatusRuntimeException error = (StatusRuntimeException) responseObserver.getError();
        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(error.getMessage()).contains("Tenant");
        assertThat(error.getMessage()).contains(TENANT_1);
        assertThat(error.getMessage()).contains(TENANT_2);
        
        verify(aqlTranspiler, never()).transpile(anyString());
        verify(queryExecutor, never()).execute(any(QueryPlan.class));
    }
    
    @Test
    @DisplayName("executeQuery should fail when no tenant is authenticated")
    void executeQueryShouldFailWhenNoTenantAuthenticated() {
        // Given - no tenant set in context
        
        QueryRequest request = QueryRequest.newBuilder()
            .setTenantId(TENANT_1)
            .setQuery("source=firewall | where severity > 3")
            .setLimit(100)
            .build();
        
        TestStreamObserver<QueryResponse> responseObserver = new TestStreamObserver<>();
        
        // When
        queryGrpcService.executeQuery(request, responseObserver);
        
        // Then
        assertThat(responseObserver.isCompleted()).isFalse();
        assertThat(responseObserver.getError()).isNotNull();
        
        StatusRuntimeException error = (StatusRuntimeException) responseObserver.getError();
        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(error.getMessage()).contains("No authenticated tenant");
    }
    
    @Test
    @DisplayName("executeQuery should reject empty tenant ID")
    void executeQueryShouldRejectEmptyTenantId() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        QueryRequest request = QueryRequest.newBuilder()
            .setTenantId("") // Empty tenant ID
            .setQuery("source=firewall | where severity > 3")
            .setLimit(100)
            .build();
        
        TestStreamObserver<QueryResponse> responseObserver = new TestStreamObserver<>();
        
        // When
        queryGrpcService.executeQuery(request, responseObserver);
        
        // Then
        assertThat(responseObserver.isCompleted()).isFalse();
        assertThat(responseObserver.getError()).isNotNull();
        
        StatusRuntimeException error = (StatusRuntimeException) responseObserver.getError();
        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(error.getMessage()).contains("Tenant ID is required");
    }
    
    @Test
    @DisplayName("executeQueryStream should succeed when tenant IDs match")
    void executeQueryStreamShouldSucceedWhenTenantIdsMatch() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        QueryRequest request = QueryRequest.newBuilder()
            .setTenantId(TENANT_1)
            .setQuery("source=firewall | where severity > 3")
            .setLimit(100)
            .build();
        
        QueryPlan queryPlan = new QueryPlan();
        QueryResult queryResult = createQueryResult(TENANT_1, 10);
        
        when(aqlTranspiler.transpile(anyString())).thenReturn(queryPlan);
        when(queryExecutor.execute(any(QueryPlan.class))).thenReturn(Flux.just(queryResult));
        
        TestStreamObserver<QueryResponse> responseObserver = new TestStreamObserver<>();
        
        // When
        queryGrpcService.executeQueryStream(request, responseObserver);
        
        // Give the async operation time to complete
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Then
        assertThat(responseObserver.isCompleted()).isTrue();
        assertThat(responseObserver.getError()).isNull();
        
        verify(aqlTranspiler).transpile("source=firewall | where severity > 3");
        verify(queryExecutor).execute(queryPlan);
    }
    
    @Test
    @DisplayName("executeQueryStream should reject when tenant IDs do not match")
    void executeQueryStreamShouldRejectWhenTenantIdsDoNotMatch() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        QueryRequest request = QueryRequest.newBuilder()
            .setTenantId(TENANT_2) // Different tenant
            .setQuery("source=firewall | where severity > 3")
            .setLimit(100)
            .build();
        
        TestStreamObserver<QueryResponse> responseObserver = new TestStreamObserver<>();
        
        // When
        queryGrpcService.executeQueryStream(request, responseObserver);
        
        // Then
        assertThat(responseObserver.isCompleted()).isFalse();
        assertThat(responseObserver.getError()).isNotNull();
        
        StatusRuntimeException error = (StatusRuntimeException) responseObserver.getError();
        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(error.getMessage()).contains("Tenant");
        
        verify(aqlTranspiler, never()).transpile(anyString());
        verify(queryExecutor, never()).execute(any(QueryPlan.class));
    }
    
    @Test
    @DisplayName("executeQuery should handle case-sensitive tenant IDs")
    void executeQueryShouldHandleCaseSensitiveTenantIds() {
        // Given
        TenantContext.setTenantId("Tenant-123");
        
        QueryRequest request = QueryRequest.newBuilder()
            .setTenantId("tenant-123") // Different case
            .setQuery("source=firewall | where severity > 3")
            .setLimit(100)
            .build();
        
        TestStreamObserver<QueryResponse> responseObserver = new TestStreamObserver<>();
        
        // When
        queryGrpcService.executeQuery(request, responseObserver);
        
        // Then - should reject due to case mismatch
        assertThat(responseObserver.isCompleted()).isFalse();
        assertThat(responseObserver.getError()).isNotNull();
        
        StatusRuntimeException error = (StatusRuntimeException) responseObserver.getError();
        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }
    
    @Test
    @DisplayName("executeQuery should include error details in Status")
    void executeQueryShouldIncludeErrorDetailsInStatus() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        QueryRequest request = QueryRequest.newBuilder()
            .setTenantId(TENANT_2)
            .setQuery("source=firewall | where severity > 3")
            .setLimit(100)
            .build();
        
        TestStreamObserver<QueryResponse> responseObserver = new TestStreamObserver<>();
        
        // When
        queryGrpcService.executeQuery(request, responseObserver);
        
        // Then
        assertThat(responseObserver.getError()).isNotNull();
        
        StatusRuntimeException error = (StatusRuntimeException) responseObserver.getError();
        
        // Verify error details are included
        com.google.rpc.Status status = io.grpc.protobuf.StatusProto.fromThrowable(error);
        assertThat(status).isNotNull();
        assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED.value());
        assertThat(status.getMessage()).contains("Tenant");
        
        // Verify ErrorInfo is included in details
        boolean hasErrorInfo = status.getDetailsList().stream()
            .anyMatch(any -> any.is(com.google.rpc.ErrorInfo.class));
        assertThat(hasErrorInfo).isTrue();
    }
    
    /**
     * Helper method to create test query results
     */
    private QueryResult createQueryResult(String tenantId, int rowCount) {
        List<Map<String, Object>> rows = new ArrayList<>();
        
        for (int i = 0; i < rowCount; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("_id", "event-" + i);
            row.put("tenant_id", tenantId);
            row.put("severity", 4);
            row.put("message", "Test event " + i);
            rows.add(row);
        }
        
        QueryResult result = new QueryResult(rows);
        result.setExecutionTimeMs(50);
        result.setStorageTier(StorageTier.HOT);
        result.setTimedOut(false);
        
        return result;
    }
    
    /**
     * Test implementation of StreamObserver for capturing responses
     */
    private static class TestStreamObserver<T> implements StreamObserver<T> {
        private final List<T> values = new ArrayList<>();
        private Throwable error;
        private boolean completed = false;
        
        @Override
        public void onNext(T value) {
            values.add(value);
        }
        
        @Override
        public void onError(Throwable t) {
            this.error = t;
        }
        
        @Override
        public void onCompleted() {
            this.completed = true;
        }
        
        public List<T> getValues() {
            return values;
        }
        
        public Throwable getError() {
            return error;
        }
        
        public boolean isCompleted() {
            return completed;
        }
    }
}
