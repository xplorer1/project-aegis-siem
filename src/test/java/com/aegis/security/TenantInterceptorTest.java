package com.aegis.security;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for TenantInterceptor
 * 
 * These tests verify that the TenantInterceptor correctly:
 * - Extracts tenant ID from JWT tokens
 * - Sets TenantContext for valid requests
 * - Rejects requests with missing or invalid tokens
 * - Clears TenantContext after request completion
 * - Handles various token formats
 * - Provides appropriate error responses
 */
@DisplayName("TenantInterceptor Tests")
class TenantInterceptorTest {
    
    @Mock
    private HttpServletRequest request;
    
    @Mock
    private HttpServletResponse response;
    
    @Mock
    private Object handler;
    
    private TenantInterceptor interceptor;
    private StringWriter responseWriter;
    
    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        interceptor = new TenantInterceptor();
        responseWriter = new StringWriter();
        
        // Setup default mock behavior
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURI()).thenReturn("/api/test");
        when(response.getWriter()).thenReturn(new PrintWriter(responseWriter));
        
        // Clear any existing tenant context
        TenantContext.clear();
    }
    
    @AfterEach
    void tearDown() {
        // Always clear tenant context after each test
        TenantContext.clear();
    }
    
    @Test
    @DisplayName("Should extract tenant ID from Bearer token and set context")
    void testPreHandle_WithValidBearerToken() throws Exception {
        // Given: A request with a valid Bearer token
        String token = "tenant-123-abc456def";
        when(request.getHeader("Authorization")).thenReturn("Bearer " + token);
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Request should proceed and tenant context should be set
        assertThat(result).isTrue();
        assertThat(TenantContext.getTenantId()).isEqualTo("tenant-123");
        verify(response, never()).setStatus(anyInt());
    }
    
    @Test
    @DisplayName("Should extract tenant ID from token without Bearer prefix")
    void testPreHandle_WithTokenWithoutBearer() throws Exception {
        // Given: A request with a token without Bearer prefix
        String token = "tenant-456-xyz789ghi";
        when(request.getHeader("Authorization")).thenReturn(token);
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Request should proceed and tenant context should be set
        assertThat(result).isTrue();
        assertThat(TenantContext.getTenantId()).thenReturn("tenant-456");
        verify(response, never()).setStatus(anyInt());
    }
    
    @Test
    @DisplayName("Should use default tenant for non-standard token format")
    void testPreHandle_WithNonStandardToken() throws Exception {
        // Given: A request with a non-standard token format
        when(request.getHeader("Authorization")).thenReturn("Bearer some-random-token");
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Request should proceed with default tenant
        assertThat(result).isTrue();
        assertThat(TenantContext.getTenantId()).isEqualTo("default-tenant");
        verify(response, never()).setStatus(anyInt());
    }
    
    @Test
    @DisplayName("Should reject request with missing Authorization header")
    void testPreHandle_WithMissingAuthHeader() throws Exception {
        // Given: A request without Authorization header
        when(request.getHeader("Authorization")).thenReturn(null);
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Request should be rejected
        assertThat(result).isFalse();
        assertThat(TenantContext.getTenantId()).isNull();
        verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(response).setContentType("application/json");
        assertThat(responseWriter.toString()).contains("Authorization header is required");
    }
    
    @Test
    @DisplayName("Should reject request with empty Authorization header")
    void testPreHandle_WithEmptyAuthHeader() throws Exception {
        // Given: A request with empty Authorization header
        when(request.getHeader("Authorization")).thenReturn("   ");
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Request should be rejected
        assertThat(result).isFalse();
        assertThat(TenantContext.getTenantId()).isNull();
        verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(response).setContentType("application/json");
        assertThat(responseWriter.toString()).contains("Authorization header is required");
    }
    
    @Test
    @DisplayName("Should reject request with Bearer prefix but no token")
    void testPreHandle_WithBearerButNoToken() throws Exception {
        // Given: A request with Bearer prefix but no token
        when(request.getHeader("Authorization")).thenReturn("Bearer   ");
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Request should be rejected
        assertThat(result).isFalse();
        assertThat(TenantContext.getTenantId()).isNull();
        verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(response).setContentType("application/json");
        assertThat(responseWriter.toString()).contains("Invalid authorization token format");
    }
    
    @Test
    @DisplayName("Should handle Bearer prefix with different casing")
    void testPreHandle_WithBearerDifferentCasing() throws Exception {
        // Given: A request with Bearer in different casing
        String token = "tenant-789-def123ghi";
        when(request.getHeader("Authorization")).thenReturn("bearer " + token);
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Request should proceed (case-insensitive Bearer handling)
        assertThat(result).isTrue();
        assertThat(TenantContext.getTenantId()).isEqualTo("tenant-789");
    }
    
    @Test
    @DisplayName("Should clear tenant context after request completion")
    void testAfterCompletion_ClearsTenantContext() throws Exception {
        // Given: Tenant context is set
        TenantContext.setTenantId("tenant-123");
        assertThat(TenantContext.getTenantId()).isEqualTo("tenant-123");
        
        // When: afterCompletion is called
        interceptor.afterCompletion(request, response, handler, null);
        
        // Then: Tenant context should be cleared
        assertThat(TenantContext.getTenantId()).isNull();
    }
    
    @Test
    @DisplayName("Should clear tenant context even when exception occurred")
    void testAfterCompletion_ClearsTenantContextWithException() throws Exception {
        // Given: Tenant context is set and an exception occurred
        TenantContext.setTenantId("tenant-456");
        Exception exception = new RuntimeException("Test exception");
        
        // When: afterCompletion is called with exception
        interceptor.afterCompletion(request, response, handler, exception);
        
        // Then: Tenant context should still be cleared
        assertThat(TenantContext.getTenantId()).isNull();
    }
    
    @Test
    @DisplayName("Should handle afterCompletion when tenant context was not set")
    void testAfterCompletion_WhenContextNotSet() throws Exception {
        // Given: Tenant context is not set
        assertThat(TenantContext.getTenantId()).isNull();
        
        // When: afterCompletion is called
        interceptor.afterCompletion(request, response, handler, null);
        
        // Then: Should not throw exception
        assertThat(TenantContext.getTenantId()).isNull();
    }
    
    @Test
    @DisplayName("Should extract tenant ID from complex token format")
    void testPreHandle_WithComplexTokenFormat() throws Exception {
        // Given: A request with a complex token format
        String token = "tenant-999-abc-def-ghi-jkl";
        when(request.getHeader("Authorization")).thenReturn("Bearer " + token);
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Should extract tenant ID correctly
        assertThat(result).isTrue();
        assertThat(TenantContext.getTenantId()).isEqualTo("tenant-999");
    }
    
    @Test
    @DisplayName("Should handle token with extra whitespace")
    void testPreHandle_WithExtraWhitespace() throws Exception {
        // Given: A request with token containing extra whitespace
        String token = "tenant-111-abc123";
        when(request.getHeader("Authorization")).thenReturn("  Bearer   " + token + "  ");
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Should handle whitespace correctly
        assertThat(result).isTrue();
        assertThat(TenantContext.getTenantId()).isEqualTo("tenant-111");
    }
    
    @Test
    @DisplayName("Should log request details for debugging")
    void testPreHandle_LogsRequestDetails() throws Exception {
        // Given: A request with valid token
        when(request.getMethod()).thenReturn("POST");
        when(request.getRequestURI()).thenReturn("/api/events");
        when(request.getHeader("Authorization")).thenReturn("Bearer tenant-222-xyz");
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Request should be processed successfully
        assertThat(result).isTrue();
        assertThat(TenantContext.getTenantId()).isEqualTo("tenant-222");
        
        // Verify request details were accessed (for logging)
        verify(request, atLeastOnce()).getMethod();
        verify(request, atLeastOnce()).getRequestURI();
    }
    
    @Test
    @DisplayName("Should handle multiple sequential requests with different tenants")
    void testPreHandle_MultipleSequentialRequests() throws Exception {
        // First request with tenant-111
        when(request.getHeader("Authorization")).thenReturn("Bearer tenant-111-abc");
        boolean result1 = interceptor.preHandle(request, response, handler);
        assertThat(result1).isTrue();
        assertThat(TenantContext.getTenantId()).isEqualTo("tenant-111");
        
        // Clear context (simulating end of request)
        interceptor.afterCompletion(request, response, handler, null);
        assertThat(TenantContext.getTenantId()).isNull();
        
        // Second request with tenant-222
        when(request.getHeader("Authorization")).thenReturn("Bearer tenant-222-def");
        boolean result2 = interceptor.preHandle(request, response, handler);
        assertThat(result2).isTrue();
        assertThat(TenantContext.getTenantId()).isEqualTo("tenant-222");
        
        // Clear context
        interceptor.afterCompletion(request, response, handler, null);
        assertThat(TenantContext.getTenantId()).isNull();
    }
    
    @Test
    @DisplayName("Should provide appropriate error message for missing token")
    void testPreHandle_ErrorMessageForMissingToken() throws Exception {
        // Given: A request without Authorization header
        when(request.getHeader("Authorization")).thenReturn(null);
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Should return appropriate error message
        assertThat(result).isFalse();
        String errorResponse = responseWriter.toString();
        assertThat(errorResponse).contains("error");
        assertThat(errorResponse).contains("Authorization header is required");
    }
    
    @Test
    @DisplayName("Should provide appropriate error message for invalid token format")
    void testPreHandle_ErrorMessageForInvalidFormat() throws Exception {
        // Given: A request with invalid token format
        when(request.getHeader("Authorization")).thenReturn("Bearer ");
        
        // When: preHandle is called
        boolean result = interceptor.preHandle(request, response, handler);
        
        // Then: Should return appropriate error message
        assertThat(result).isFalse();
        String errorResponse = responseWriter.toString();
        assertThat(errorResponse).contains("error");
        assertThat(errorResponse).contains("Invalid authorization token format");
    }
}
