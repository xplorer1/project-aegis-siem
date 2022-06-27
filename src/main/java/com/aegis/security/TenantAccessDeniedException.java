package com.aegis.security;

/**
 * Exception thrown when a tenant attempts to access resources belonging to another tenant.
 * 
 * This exception indicates a security violation where:
 * - The authenticated tenant_id (from JWT token) doesn't match
 * - The requested tenant_id (from API request parameters)
 * 
 * This enforces Requirement 9.6: Multi-Tenant Isolation
 * "THE API_Gateway SHALL validate tenant_id in authentication tokens matches 
 * tenant_id in all API requests"
 * 
 * Security Implications:
 * - This exception should be logged and monitored for security incidents
 * - Repeated violations may indicate:
 *   - Privilege escalation attempts
 *   - Compromised credentials
 *   - Application bugs
 *   - Misconfigured clients
 * 
 * HTTP Status Mapping:
 * - REST APIs: Should return 403 Forbidden
 * - GraphQL: Should return error with FORBIDDEN code
 * - gRPC: Should return PERMISSION_DENIED status
 * 
 * Example Usage:
 * <pre>
 * if (!authenticatedTenant.equals(requestTenant)) {
 *     throw new TenantAccessDeniedException(
 *         "Tenant 'tenant-123' cannot access resources for tenant 'tenant-456'"
 *     );
 * }
 * </pre>
 * 
 * @see TenantValidator
 * @see TenantContext
 */
public class TenantAccessDeniedException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * The tenant ID that was authenticated (from JWT token)
     */
    private final String authenticatedTenantId;
    
    /**
     * The tenant ID that was requested (from API request)
     */
    private final String requestedTenantId;
    
    /**
     * Constructs a new TenantAccessDeniedException with the specified detail message.
     * 
     * @param message The detail message explaining the access denial
     */
    public TenantAccessDeniedException(String message) {
        super(message);
        this.authenticatedTenantId = null;
        this.requestedTenantId = null;
    }
    
    /**
     * Constructs a new TenantAccessDeniedException with the specified detail message and cause.
     * 
     * @param message The detail message explaining the access denial
     * @param cause The underlying cause of the exception
     */
    public TenantAccessDeniedException(String message, Throwable cause) {
        super(message, cause);
        this.authenticatedTenantId = null;
        this.requestedTenantId = null;
    }
    
    /**
     * Constructs a new TenantAccessDeniedException with tenant IDs and detail message.
     * 
     * This constructor captures both the authenticated and requested tenant IDs
     * for detailed logging and security monitoring.
     * 
     * @param authenticatedTenantId The tenant ID from the authentication token
     * @param requestedTenantId The tenant ID from the API request
     * @param message The detail message explaining the access denial
     */
    public TenantAccessDeniedException(
            String authenticatedTenantId, 
            String requestedTenantId, 
            String message) {
        super(message);
        this.authenticatedTenantId = authenticatedTenantId;
        this.requestedTenantId = requestedTenantId;
    }
    
    /**
     * Gets the authenticated tenant ID (from JWT token).
     * 
     * @return The authenticated tenant ID, or null if not set
     */
    public String getAuthenticatedTenantId() {
        return authenticatedTenantId;
    }
    
    /**
     * Gets the requested tenant ID (from API request).
     * 
     * @return The requested tenant ID, or null if not set
     */
    public String getRequestedTenantId() {
        return requestedTenantId;
    }
    
    /**
     * Returns a detailed message including tenant IDs if available.
     * 
     * @return A detailed error message
     */
    @Override
    public String getMessage() {
        String baseMessage = super.getMessage();
        
        if (authenticatedTenantId != null && requestedTenantId != null) {
            return String.format("%s [authenticated=%s, requested=%s]",
                baseMessage, authenticatedTenantId, requestedTenantId);
        }
        
        return baseMessage;
    }
}
