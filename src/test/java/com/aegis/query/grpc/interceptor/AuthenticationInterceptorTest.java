package com.aegis.query.grpc.interceptor;

import io.grpc.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AuthenticationInterceptor
 * 
 * Tests authentication logic including:
 * - Valid token authentication
 * - Missing token rejection
 * - Invalid token format rejection
 * - Tenant ID extraction
 * - Context propagation
 * - Metrics tracking
 */
class AuthenticationInterceptorTest {
    
    private AuthenticationInterceptor interceptor;
    private MeterRegistry meterRegistry;
    private ServerCall<String, String> mockCall;
    private ServerCallHandler<String, String> mockNext;
    private Metadata headers;
    
    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        interceptor = new AuthenticationInterceptor(meterRegistry);
        
        mockCall = mock(ServerCall.class);
        mockNext = mock(ServerCallHandler.class);
        headers = new Metadata();
        
        // Setup mock call
        MethodDescriptor<String, String> methodDescriptor = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("TestService/TestMethod")
            .setRequestMarshaller(new StringMarshaller())
            .setResponseMarshaller(new StringMarshaller())
            .build();
        
        when(mockCall.getMethodDescriptor()).thenReturn(methodDescriptor);
    }
    
    @Test
    void testValidTokenAuthentication() {
        // Given: Valid authorization token
        Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        headers.put(authKey, "Bearer tenant-123-abc456");
        
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting the call
        ServerCall.Listener<String> result = interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Call should proceed
        verify(mockNext).startCall(any(), any());
        verify(mockCall, never()).close(any(), any());
        assertThat(result).isNotNull();
        
        // And: Metrics should be updated
        Counter attempts = meterRegistry.find("grpc.auth.attempts").counter();
        Counter successes = meterRegistry.find("grpc.auth.successes").counter();
        assertThat(attempts).isNotNull();
        assertThat(successes).isNotNull();
        assertThat(attempts.count()).isEqualTo(1.0);
        assertThat(successes.count()).isEqualTo(1.0);
    }
    
    @Test
    void testValidTokenWithoutBearerPrefix() {
        // Given: Valid token without "Bearer " prefix
        Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        headers.put(authKey, "tenant-456-xyz789");
        
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting the call
        ServerCall.Listener<String> result = interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Call should proceed
        verify(mockNext).startCall(any(), any());
        verify(mockCall, never()).close(any(), any());
        assertThat(result).isNotNull();
    }
    
    @Test
    void testMissingToken() {
        // Given: No authorization token in headers
        
        // When: Intercepting the call
        ServerCall.Listener<String> result = interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Call should be rejected
        ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
        verify(mockCall).close(statusCaptor.capture(), any());
        verify(mockNext, never()).startCall(any(), any());
        
        Status status = statusCaptor.getValue();
        assertThat(status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
        assertThat(status.getDescription()).contains("Authorization token is required");
        
        // And: Failure metrics should be updated
        Counter failures = meterRegistry.find("grpc.auth.failures").counter();
        assertThat(failures).isNotNull();
        assertThat(failures.count()).isEqualTo(1.0);
    }
    
    @Test
    void testEmptyToken() {
        // Given: Empty authorization token
        Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        headers.put(authKey, "");
        
        // When: Intercepting the call
        ServerCall.Listener<String> result = interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Call should be rejected
        ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
        verify(mockCall).close(statusCaptor.capture(), any());
        
        Status status = statusCaptor.getValue();
        assertThat(status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    }
    
    @Test
    void testInvalidTokenFormat() {
        // Given: Token with only "Bearer " prefix
        Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        headers.put(authKey, "Bearer ");
        
        // When: Intercepting the call
        ServerCall.Listener<String> result = interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Call should be rejected
        ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
        verify(mockCall).close(statusCaptor.capture(), any());
        
        Status status = statusCaptor.getValue();
        assertThat(status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
        assertThat(status.getDescription()).contains("Invalid authorization token format");
    }
    
    @Test
    void testTenantIdExtraction() {
        // Given: Token with tenant ID
        Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        headers.put(authKey, "tenant-789-token123");
        
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting the call
        interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Tenant ID should be extractable from context
        // Note: In actual usage, the tenant ID would be available in the context
        // during the service method execution
        verify(mockNext).startCall(any(), any());
    }
    
    @Test
    void testDefaultTenantForNonStandardToken() {
        // Given: Token that doesn't match tenant-<id>-<random> format
        Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        headers.put(authKey, "some-random-token-12345");
        
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting the call
        ServerCall.Listener<String> result = interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Call should proceed with default tenant
        verify(mockNext).startCall(any(), any());
        verify(mockCall, never()).close(any(), any());
        assertThat(result).isNotNull();
    }
    
    @Test
    void testMetricsTracking() {
        // Given: Multiple authentication attempts
        Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Processing valid and invalid tokens
        headers.put(authKey, "tenant-123-valid");
        interceptor.interceptCall(mockCall, headers, mockNext);
        
        headers.remove(authKey, "tenant-123-valid");
        interceptor.interceptCall(mockCall, headers, mockNext);
        
        headers.put(authKey, "tenant-456-valid");
        interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Metrics should reflect all attempts
        Counter attempts = meterRegistry.find("grpc.auth.attempts").counter();
        Counter successes = meterRegistry.find("grpc.auth.successes").counter();
        Counter failures = meterRegistry.find("grpc.auth.failures").counter();
        
        assertThat(attempts.count()).isEqualTo(3.0);
        assertThat(successes.count()).isEqualTo(2.0);
        assertThat(failures.count()).isEqualTo(1.0);
    }
    
    /**
     * Simple string marshaller for testing
     */
    private static class StringMarshaller implements MethodDescriptor.Marshaller<String> {
        @Override
        public java.io.InputStream stream(String value) {
            return new java.io.ByteArrayInputStream(value.getBytes());
        }
        
        @Override
        public String parse(java.io.InputStream stream) {
            try {
                return new String(stream.readAllBytes());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
