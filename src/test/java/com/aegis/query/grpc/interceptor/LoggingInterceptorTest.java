package com.aegis.query.grpc.interceptor;

import io.grpc.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for LoggingInterceptor
 * 
 * Tests logging and metrics collection including:
 * - Request logging
 * - Response logging
 * - Error logging
 * - Metrics tracking
 * - Status code handling
 * - Cancellation handling
 */
class LoggingInterceptorTest {
    
    private LoggingInterceptor interceptor;
    private MeterRegistry meterRegistry;
    private ServerCall<String, String> mockCall;
    private ServerCallHandler<String, String> mockNext;
    private Metadata headers;
    
    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        interceptor = new LoggingInterceptor(meterRegistry);
        
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
    void testSuccessfulRequestLogging() {
        // Given: A successful request
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting the call
        ServerCall.Listener<String> result = interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Listener should be returned
        assertThat(result).isNotNull();
        
        // And: Request metrics should be incremented
        Counter totalRequests = meterRegistry.find("grpc.requests.total").counter();
        assertThat(totalRequests).isNotNull();
        assertThat(totalRequests.count()).isEqualTo(1.0);
    }
    
    @Test
    void testRequestWithUserAgent() {
        // Given: Request with user-agent header
        Metadata.Key<String> userAgentKey = Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER);
        headers.put(userAgentKey, "grpc-java/1.50.0");
        
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting the call
        ServerCall.Listener<String> result = interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Call should proceed normally
        assertThat(result).isNotNull();
        verify(mockNext).startCall(any(), any());
    }
    
    @Test
    void testResponseLogging() {
        // Given: A call that will complete successfully
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        ArgumentCaptor<ServerCall> callCaptor = ArgumentCaptor.forClass(ServerCall.class);
        when(mockNext.startCall(callCaptor.capture(), any())).thenReturn(mockListener);
        
        // When: Intercepting and completing the call
        interceptor.interceptCall(mockCall, headers, mockNext);
        ServerCall<String, String> wrappedCall = callCaptor.getValue();
        wrappedCall.close(Status.OK, new Metadata());
        
        // Then: Response metrics should be tracked
        Timer duration = meterRegistry.find("grpc.requests.duration").timer();
        assertThat(duration).isNotNull();
        assertThat(duration.count()).isEqualTo(1);
    }
    
    @Test
    void testErrorResponseLogging() {
        // Given: A call that will fail
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        ArgumentCaptor<ServerCall> callCaptor = ArgumentCaptor.forClass(ServerCall.class);
        when(mockNext.startCall(callCaptor.capture(), any())).thenReturn(mockListener);
        
        // When: Intercepting and failing the call
        interceptor.interceptCall(mockCall, headers, mockNext);
        ServerCall<String, String> wrappedCall = callCaptor.getValue();
        wrappedCall.close(Status.INTERNAL.withDescription("Test error"), new Metadata());
        
        // Then: Error metrics should be tracked
        Counter statusCounter = meterRegistry.find("grpc.responses.by.status")
            .tag("status", "INTERNAL")
            .counter();
        assertThat(statusCounter).isNotNull();
        assertThat(statusCounter.count()).isEqualTo(1.0);
    }
    
    @Test
    void testClientErrorLogging() {
        // Given: A call that will fail with client error
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        ArgumentCaptor<ServerCall> callCaptor = ArgumentCaptor.forClass(ServerCall.class);
        when(mockNext.startCall(callCaptor.capture(), any())).thenReturn(mockListener);
        
        // When: Intercepting and failing with INVALID_ARGUMENT
        interceptor.interceptCall(mockCall, headers, mockNext);
        ServerCall<String, String> wrappedCall = callCaptor.getValue();
        wrappedCall.close(Status.INVALID_ARGUMENT.withDescription("Invalid input"), new Metadata());
        
        // Then: Client error metrics should be tracked
        Counter statusCounter = meterRegistry.find("grpc.responses.by.status")
            .tag("status", "INVALID_ARGUMENT")
            .counter();
        assertThat(statusCounter).isNotNull();
        assertThat(statusCounter.count()).isEqualTo(1.0);
    }
    
    @Test
    void testMessageCounting() {
        // Given: A streaming call
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        ArgumentCaptor<ServerCall> callCaptor = ArgumentCaptor.forClass(ServerCall.class);
        when(mockNext.startCall(callCaptor.capture(), any())).thenReturn(mockListener);
        
        // When: Intercepting and sending multiple messages
        interceptor.interceptCall(mockCall, headers, mockNext);
        ServerCall<String, String> wrappedCall = callCaptor.getValue();
        
        wrappedCall.sendMessage("message1");
        wrappedCall.sendMessage("message2");
        wrappedCall.sendMessage("message3");
        wrappedCall.close(Status.OK, new Metadata());
        
        // Then: Messages should be counted (verified through logging)
        // Note: Message count is logged but not exposed as a separate metric
        verify(mockCall, times(3)).sendMessage(anyString());
    }
    
    @Test
    void testCancellationLogging() {
        // Given: A call that will be cancelled
        ArgumentCaptor<ServerCall.Listener> listenerCaptor = ArgumentCaptor.forClass(ServerCall.Listener.class);
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting the call
        ServerCall.Listener<String> wrappedListener = interceptor.interceptCall(mockCall, headers, mockNext);
        
        // And: Cancelling the call
        wrappedListener.onCancel();
        
        // Then: Cancellation metrics should be tracked
        Counter cancelCounter = meterRegistry.find("grpc.requests.cancelled")
            .tag("method", "TestService/TestMethod")
            .counter();
        assertThat(cancelCounter).isNotNull();
        assertThat(cancelCounter.count()).isEqualTo(1.0);
    }
    
    @Test
    void testMessageReceiving() {
        // Given: A call with incoming messages
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting and receiving messages
        ServerCall.Listener<String> wrappedListener = interceptor.interceptCall(mockCall, headers, mockNext);
        
        wrappedListener.onMessage("test message 1");
        wrappedListener.onMessage("test message 2");
        
        // Then: Messages should be forwarded to the actual listener
        verify(mockListener, times(2)).onMessage(anyString());
    }
    
    @Test
    void testHalfClose() {
        // Given: A call that will half-close
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting and half-closing
        ServerCall.Listener<String> wrappedListener = interceptor.interceptCall(mockCall, headers, mockNext);
        wrappedListener.onHalfClose();
        
        // Then: Half-close should be forwarded
        verify(mockListener).onHalfClose();
    }
    
    @Test
    void testComplete() {
        // Given: A call that will complete
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting and completing
        ServerCall.Listener<String> wrappedListener = interceptor.interceptCall(mockCall, headers, mockNext);
        wrappedListener.onComplete();
        
        // Then: Complete should be forwarded
        verify(mockListener).onComplete();
    }
    
    @Test
    void testMultipleRequests() {
        // Given: Multiple requests
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        ArgumentCaptor<ServerCall> callCaptor = ArgumentCaptor.forClass(ServerCall.class);
        when(mockNext.startCall(callCaptor.capture(), any())).thenReturn(mockListener);
        
        // When: Processing multiple requests
        interceptor.interceptCall(mockCall, headers, mockNext);
        ServerCall<String, String> wrappedCall1 = callCaptor.getValue();
        wrappedCall1.close(Status.OK, new Metadata());
        
        interceptor.interceptCall(mockCall, headers, mockNext);
        ServerCall<String, String> wrappedCall2 = callCaptor.getValue();
        wrappedCall2.close(Status.INTERNAL, new Metadata());
        
        interceptor.interceptCall(mockCall, headers, mockNext);
        ServerCall<String, String> wrappedCall3 = callCaptor.getValue();
        wrappedCall3.close(Status.OK, new Metadata());
        
        // Then: All requests should be tracked
        Counter totalRequests = meterRegistry.find("grpc.requests.total").counter();
        assertThat(totalRequests.count()).isEqualTo(3.0);
        
        Counter okResponses = meterRegistry.find("grpc.responses.by.status")
            .tag("status", "OK")
            .counter();
        assertThat(okResponses.count()).isEqualTo(2.0);
        
        Counter errorResponses = meterRegistry.find("grpc.responses.by.status")
            .tag("status", "INTERNAL")
            .counter();
        assertThat(errorResponses.count()).isEqualTo(1.0);
    }
    
    @Test
    void testMethodTagging() {
        // Given: Request to specific method
        ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
        when(mockNext.startCall(any(), any())).thenReturn(mockListener);
        
        // When: Intercepting the call
        interceptor.interceptCall(mockCall, headers, mockNext);
        
        // Then: Method should be tagged in metrics
        Counter methodCounter = meterRegistry.find("grpc.requests.by.method")
            .tag("method", "TestService/TestMethod")
            .counter();
        assertThat(methodCounter).isNotNull();
        assertThat(methodCounter.count()).isEqualTo(1.0);
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
