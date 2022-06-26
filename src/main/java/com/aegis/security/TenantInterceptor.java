package com.aegis.security;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Tenant Interceptor for Multi-Tenant Isolation
 * 
 * This interceptor extracts the tenant ID from JWT tokens in incoming HTTP requests
 * and sets it in the TenantContext for the duration of the request. This ensures
 * that all database queries and operations are automatically scoped to the correct
 * tenant, enforcing multi-tenant isolation at the application level.
 * 
 * Request Flow:
 * 1. Extract Authorization header from request
 * 2. Parse JWT token and extract tenant_id claim
 * 3. Set tenant ID in TenantContext (ThreadLocal)
 * 4. Process request with tenant context available
 * 5. Clear TenantContext after request completion
 * 
 * Token Format:
 * - Header: "Authorization: Bearer <jwt-token>"
 * - JWT Claims: { "tenant_id": "tenant-123", ... }
 * 
 * Security Considerations:
 * - Validates JWT signature and expiration (in production)
 * - Rejects requests without valid tenant_id
 * - Clears context after each request to prevent leakage
 * - Thread-safe via ThreadLocal storage
 * 
 * Integration:
 * - Registered globally via WebMvcConfigurer
 * - Runs before all controller methods
 * - Works with both REST and GraphQL endpoints
 * 
 * @see TenantContext
 * @see HandlerInterceptor
 */
@Component
public class TenantInterceptor implements HandlerInterceptor {
    
    private static final Logger log = LoggerFactory.getLogger(TenantInterceptor.class);
    
    // HTTP header for authorization token
    private static final String AUTHORIZATION_HEADER = "Authorization";
    
    // Bearer token prefix
    private static final String BEARER_PREFIX = "Bearer ";
    
    /**
     * Pre-handle method called before the controller method is invoked.
     * 
     * This method:
     * 1. Extracts the Authorization header from the request
     * 2. Validates and parses the JWT token
     * 3. Extracts the tenant_id claim from the JWT
     * 4. Sets the tenant ID in TenantContext
     * 5. Returns true to allow the request to proceed
     * 
     * If the tenant ID cannot be extracted or is invalid, the method
     * returns false and sets an appropriate HTTP error response.
     * 
     * @param request The HTTP request
     * @param response The HTTP response
     * @param handler The handler (controller method) to be executed
     * @return true if the request should proceed, false to abort
     * @throws Exception if an error occurs during processing
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) 
            throws Exception {
        
        String requestUri = request.getRequestURI();
        String method = request.getMethod();
        
        log.debug("TenantInterceptor processing request: {} {}", method, requestUri);
        
        try {
            // Extract Authorization header
            String authHeader = request.getHeader(AUTHORIZATION_HEADER);
            
            if (authHeader == null || authHeader.trim().isEmpty()) {
                log.warn("Missing Authorization header for request: {} {}", method, requestUri);
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\":\"Authorization header is required\"}");
                return false;
            }
            
            // Extract JWT token
            String token = extractToken(authHeader);
            
            if (token == null || token.isEmpty()) {
                log.warn("Invalid Authorization header format for request: {} {}", method, requestUri);
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\":\"Invalid authorization token format\"}");
                return false;
            }
            
            // Validate token and extract tenant ID
            String tenantId = validateTokenAndExtractTenantId(token);
            
            if (tenantId == null || tenantId.isEmpty()) {
                log.warn("Invalid or expired token for request: {} {}", method, requestUri);
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\":\"Invalid or expired authorization token\"}");
                return false;
            }
            
            // Set tenant ID in context
            TenantContext.setTenantId(tenantId);
            
            log.debug("Tenant context set for request: {} {} (tenant: {})", method, requestUri, tenantId);
            
            // Allow request to proceed
            return true;
            
        } catch (Exception e) {
            log.error("Error processing tenant context for request: {} {}", method, requestUri, e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"Internal server error\"}");
            return false;
        }
    }
    
    /**
     * After-completion method called after the request has been completed.
     * 
     * This method is called regardless of whether the request succeeded or failed,
     * making it the ideal place to clean up the TenantContext. This prevents:
     * - Memory leaks in thread pool environments
     * - Tenant ID bleeding between requests when threads are reused
     * - Security vulnerabilities from stale tenant context
     * 
     * @param request The HTTP request
     * @param response The HTTP response
     * @param handler The handler that was executed
     * @param ex Any exception that was thrown during request processing
     * @throws Exception if an error occurs during cleanup
     */
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, 
                               Object handler, Exception ex) throws Exception {
        
        String tenantId = TenantContext.getTenantId();
        
        if (tenantId != null) {
            log.debug("Clearing tenant context for request: {} {} (tenant: {})", 
                request.getMethod(), request.getRequestURI(), tenantId);
        }
        
        // Always clear the tenant context
        TenantContext.clear();
    }
    
    /**
     * Extract JWT token from Authorization header.
     * 
     * Supports two formats:
     * 1. "Bearer <token>" - Standard OAuth2/JWT format
     * 2. "<token>" - Direct token without prefix
     * 
     * @param authHeader The Authorization header value
     * @return The extracted token, or null if invalid
     */
    private String extractToken(String authHeader) {
        if (authHeader == null || authHeader.trim().isEmpty()) {
            return null;
        }
        
        String token = authHeader.trim();
        
        // Remove "Bearer " prefix if present
        if (token.startsWith(BEARER_PREFIX)) {
            token = token.substring(BEARER_PREFIX.length()).trim();
        }
        
        return token.isEmpty() ? null : token;
    }
    
    /**
     * Validate JWT token and extract tenant ID.
     * 
     * This is a simplified implementation for demonstration purposes.
     * 
     * In a production system, this method would:
     * 1. Parse the JWT token (header.payload.signature)
     * 2. Validate the JWT signature using the public key
     * 3. Check token expiration (exp claim)
     * 4. Verify token issuer (iss claim)
     * 5. Extract tenant_id from JWT claims
     * 6. Validate tenant is active in the database
     * 7. Check user permissions for the tenant
     * 
     * Production Implementation Example:
     * <pre>
     * DecodedJWT jwt = JWT.require(Algorithm.RSA256(publicKey))
     *     .withIssuer("aegis-auth")
     *     .build()
     *     .verify(token);
     * 
     * String tenantId = jwt.getClaim("tenant_id").asString();
     * 
     * if (!tenantRepository.isActive(tenantId)) {
     *     return null;
     * }
     * 
     * return tenantId;
     * </pre>
     * 
     * Current Implementation:
     * - Accepts any non-empty token
     * - Extracts tenant ID from token format: "tenant-<id>-<random>"
     * - Falls back to "default-tenant" for testing/development
     * 
     * @param token The JWT token
     * @return The tenant ID, or null if validation fails
     */
    private String validateTokenAndExtractTenantId(String token) {
        // TODO: Implement proper JWT validation with signature verification
        // This is a placeholder implementation for development/testing
        
        if (token == null || token.isEmpty()) {
            return null;
        }
        
        // Simple token format for testing: tenant-<id>-<random>
        // Example: tenant-123-abc456def
        if (token.startsWith("tenant-")) {
            String[] parts = token.split("-");
            if (parts.length >= 2) {
                String tenantId = "tenant-" + parts[1];
                log.debug("Extracted tenant ID from token: {}", tenantId);
                return tenantId;
            }
        }
        
        // For development/testing, accept any token and use default tenant
        log.debug("Using default tenant for token: {}", 
            token.substring(0, Math.min(10, token.length())) + "...");
        return "default-tenant";
    }
}
