package com.aegis.query.grpc.interceptor;

import io.grpc.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * gRPC Authentication Interceptor
 * 
 * This interceptor validates authentication tokens for all incoming gRPC requests.
 * It extracts the authentication token from the request metadata, validates it,
 * and sets the tenant context for downstream processing.
 * 
 * Features:
 * - Token extraction from gRPC metadata
 * - Token validation (format and presence)
 * - Tenant ID extraction from token
 * - Request rejection for invalid/missing tokens
 * - Metrics tracking for authentication attempts
 * 
 * The interceptor follows the gRPC ServerInterceptor pattern and is automatically
 * applied to all gRPC services via the @GrpcGlobalServerInterceptor annotation.
 * 
 * Token Format:
 * - Metadata key: "authorization"
 * - Value format: "Bearer <token>" or just "<token>"
 * 
 * In a production system, this would integrate with:
 * - JWT validation
 * - OAuth2/OIDC providers
 * - API key management systems
 * - Tenant database for token-to-tenant mapping
 */
@Component
@GrpcGlobalServerInterceptor
public class AuthenticationInterceptor implements ServerInterceptor {
    
    private static final Logger log = LoggerFactory.getLogger(AuthenticationInterceptor.class);
    
    // Metadata key for authorization token
    private static final Metadata.Key<String> AUTHORIZATION_KEY = 
        Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    
    // Context key for storing tenant ID
    public static final Context.Key<String> TENANT_ID_CONTEXT_KEY = 
        Context.key("tenant_id");
    
    // Metrics
    private final Counter authenticationAttempts;
    private final Counter authenticationSuccesses;
    private final Counter authenticationFailures;
    
    /**
     * Constructor with dependency injection
     * 
     * @param meterRegistry Metrics registry for tracking authentication metrics
     */
    public AuthenticationInterceptor(MeterRegistry meterRegistry) {
        this.authenticationAttempts = Counter.builder("grpc.auth.attempts")
            .description("Total gRPC authentication attempts")
            .register(meterRegistry);
        
        this.authenticationSuccesses = Counter.builder("grpc.auth.successes")
            .description("Total successful gRPC authentications")
            .register(meterRegistry);
        
        this.authenticationFailures = Counter.builder("grpc.auth.failures")
            .description("Total failed gRPC authentications")
            .register(meterRegistry);
        
        log.info("AuthenticationInterceptor initialized");
    }
    
    /**
     * Intercept gRPC calls to validate authentication
     * 
     * This method is called for every incoming gRPC request before it reaches
     * the service implementation. It:
     * 1. Extracts the authorization token from metadata
     * 2. Validates the token format and presence
     * 3. Extracts the tenant ID from the token
     * 4. Sets the tenant ID in the gRPC context
     * 5. Allows the call to proceed or rejects it with UNAUTHENTICATED status
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
        
        authenticationAttempts.increment();
        
        String methodName = call.getMethodDescriptor().getFullMethodName();
        log.debug("Authenticating gRPC call: {}", methodName);
        
        try {
            // Extract authorization token from metadata
            String authToken = headers.get(AUTHORIZATION_KEY);
            
            if (authToken == null || authToken.trim().isEmpty()) {
                log.warn("Missing authorization token for gRPC call: {}", methodName);
                authenticationFailures.increment();
                
                call.close(
                    Status.UNAUTHENTICATED
                        .withDescription("Authorization token is required"),
                    new Metadata()
                );
                
                return new ServerCall.Listener<ReqT>() {};
            }
            
            // Extract token (remove "Bearer " prefix if present)
            String token = extractToken(authToken);
            
            if (token == null || token.isEmpty()) {
                log.warn("Invalid authorization token format for gRPC call: {}", methodName);
                authenticationFailures.increment();
                
                call.close(
                    Status.UNAUTHENTICATED
                        .withDescription("Invalid authorization token format"),
                    new Metadata()
                );
                
                return new ServerCall.Listener<ReqT>() {};
            }
            
            // Validate token and extract tenant ID
            // In production, this would:
            // - Validate JWT signature
            // - Check token expiration
            // - Verify token against database/cache
            // - Extract claims (tenant_id, user_id, roles, etc.)
            String tenantId = validateTokenAndExtractTenantId(token);
            
            if (tenantId == null || tenantId.isEmpty()) {
                log.warn("Invalid or expired token for gRPC call: {}", methodName);
                authenticationFailures.increment();
                
                call.close(
                    Status.UNAUTHENTICATED
                        .withDescription("Invalid or expired authorization token"),
                    new Metadata()
                );
                
                return new ServerCall.Listener<ReqT>() {};
            }
            
            // Authentication successful - set tenant ID in context
            Context context = Context.current()
                .withValue(TENANT_ID_CONTEXT_KEY, tenantId);
            
            authenticationSuccesses.increment();
            log.debug("Authentication successful for tenant: {} on call: {}", tenantId, methodName);
            
            // Proceed with the call in the authenticated context
            return Contexts.interceptCall(context, call, headers, next);
            
        } catch (Exception e) {
            log.error("Authentication error for gRPC call: {}", methodName, e);
            authenticationFailures.increment();
            
            call.close(
                Status.INTERNAL
                    .withDescription("Authentication error: " + e.getMessage())
                    .withCause(e),
                new Metadata()
            );
            
            return new ServerCall.Listener<ReqT>() {};
        }
    }
    
    /**
     * Extract token from authorization header
     * 
     * Supports formats:
     * - "Bearer <token>"
     * - "<token>"
     * 
     * @param authHeader The authorization header value
     * @return The extracted token, or null if invalid
     */
    private String extractToken(String authHeader) {
        if (authHeader == null || authHeader.trim().isEmpty()) {
            return null;
        }
        
        String token = authHeader.trim();
        
        // Remove "Bearer " prefix if present
        if (token.toLowerCase().startsWith("bearer ")) {
            token = token.substring(7).trim();
        }
        
        return token.isEmpty() ? null : token;
    }
    
    /**
     * Validate token and extract tenant ID
     * 
     * This is a simplified implementation for demonstration.
     * In production, this would:
     * 1. Parse and validate JWT signature
     * 2. Check token expiration
     * 3. Verify token against database or cache
     * 4. Extract tenant_id claim from JWT
     * 5. Validate tenant is active and authorized
     * 
     * Current implementation:
     * - Accepts any non-empty token
     * - Extracts tenant ID from token format: "tenant-<id>-<random>"
     * - Falls back to "default-tenant" if format doesn't match
     * 
     * @param token The authentication token
     * @return The tenant ID, or null if validation fails
     */
    private String validateTokenAndExtractTenantId(String token) {
        // TODO: Implement proper JWT validation
        // For now, accept any non-empty token and extract tenant ID
        
        if (token == null || token.isEmpty()) {
            return null;
        }
        
        // Simple token format: tenant-<id>-<random>
        // Example: tenant-123-abc456def
        if (token.startsWith("tenant-")) {
            String[] parts = token.split("-");
            if (parts.length >= 2) {
                return "tenant-" + parts[1];
            }
        }
        
        // For testing/development, accept any token and use default tenant
        log.debug("Using default tenant for token: {}", token.substring(0, Math.min(10, token.length())));
        return "default-tenant";
    }
    
    /**
     * Get tenant ID from current gRPC context
     * 
     * This is a utility method that can be called from service implementations
     * to retrieve the authenticated tenant ID.
     * 
     * @return The tenant ID, or null if not authenticated
     */
    public static String getTenantIdFromContext() {
        return TENANT_ID_CONTEXT_KEY.get();
    }
}
