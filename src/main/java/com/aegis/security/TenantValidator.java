package com.aegis.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Tenant Validator for Multi-Tenant Security
 * 
 * This component validates that the tenant_id in API requests matches the
 * tenant_id extracted from the authentication token. This enforces Requirement 9.6:
 * "THE API_Gateway SHALL validate tenant_id in authentication tokens matches 
 * tenant_id in all API requests"
 * 
 * Security Model:
 * 1. Authentication token (JWT) contains the authenticated tenant_id
 * 2. API requests may include tenant_id in request parameters/body
 * 3. The request tenant_id MUST match the authenticated tenant_id
 * 4. Mismatches indicate potential security violations or privilege escalation attempts
 * 
 * Usage:
 * <pre>
 * // In a controller or service
 * tenantValidator.validateTenantAccess(requestTenantId);
 * 
 * // Or check without throwing exception
 * if (!tenantValidator.hasAccessToTenant(requestTenantId)) {
 *     // Handle unauthorized access
 * }
 * </pre>
 * 
 * Integration Points:
 * - GraphQL controllers: Validate tenant_id in query/mutation arguments
 * - gRPC services: Validate tenant_id in request messages
 * - REST controllers: Validate tenant_id in path variables or request body
 * 
 * @see TenantContext
 * @see TenantInterceptor
 */
@Component
public class TenantValidator {
    
    private static final Logger log = LoggerFactory.getLogger(TenantValidator.class);
    
    /**
     * Validates that the request tenant ID matches the authenticated tenant ID.
     * 
     * This method:
     * 1. Retrieves the authenticated tenant_id from TenantContext
     * 2. Compares it with the requested tenant_id
     * 3. Throws TenantAccessDeniedException if they don't match
     * 4. Logs security violations for audit purposes
     * 
     * Security Considerations:
     * - Always call this method before processing tenant-specific operations
     * - Log all validation failures for security monitoring
     * - Consider rate limiting repeated validation failures
     * - Alert on potential privilege escalation attempts
     * 
     * @param requestTenantId The tenant ID from the API request
     * @throws TenantAccessDeniedException if tenant IDs don't match
     * @throws IllegalStateException if no authenticated tenant is set
     * @throws IllegalArgumentException if requestTenantId is null or empty
     */
    public void validateTenantAccess(String requestTenantId) {
        // Validate input
        if (requestTenantId == null || requestTenantId.trim().isEmpty()) {
            log.warn("Tenant validation failed: request tenant ID is null or empty");
            throw new IllegalArgumentException("Request tenant ID must not be null or empty");
        }
        
        // Get authenticated tenant from context
        String authenticatedTenantId = TenantContext.getTenantId();
        
        if (authenticatedTenantId == null) {
            log.error("Tenant validation failed: no authenticated tenant in context");
            throw new IllegalStateException("No authenticated tenant found in security context");
        }
        
        // Validate tenant access
        if (!authenticatedTenantId.equals(requestTenantId)) {
            log.warn("Tenant access denied: authenticated tenant '{}' attempted to access tenant '{}'",
                authenticatedTenantId, requestTenantId);
            
            throw new TenantAccessDeniedException(
                String.format("Access denied: tenant '%s' cannot access resources for tenant '%s'",
                    authenticatedTenantId, requestTenantId)
            );
        }
        
        log.debug("Tenant validation successful: tenant '{}' accessing own resources", 
            authenticatedTenantId);
    }
    
    /**
     * Checks if the authenticated tenant has access to the requested tenant.
     * 
     * This is a non-throwing variant of validateTenantAccess() that returns
     * a boolean instead of throwing an exception. Useful for conditional logic
     * where you want to handle unauthorized access gracefully.
     * 
     * @param requestTenantId The tenant ID from the API request
     * @return true if access is allowed, false otherwise
     */
    public boolean hasAccessToTenant(String requestTenantId) {
        try {
            validateTenantAccess(requestTenantId);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Validates that the authenticated tenant matches the expected tenant.
     * 
     * This is a convenience method for cases where you already know the
     * expected tenant ID and want to ensure the authenticated user belongs
     * to that tenant.
     * 
     * @param expectedTenantId The expected tenant ID
     * @throws TenantAccessDeniedException if tenant IDs don't match
     */
    public void requireTenant(String expectedTenantId) {
        validateTenantAccess(expectedTenantId);
    }
    
    /**
     * Gets the authenticated tenant ID from the security context.
     * 
     * This is a convenience method that wraps TenantContext.getTenantId()
     * and provides better error handling.
     * 
     * @return The authenticated tenant ID
     * @throws IllegalStateException if no tenant is authenticated
     */
    public String getAuthenticatedTenantId() {
        String tenantId = TenantContext.getTenantId();
        
        if (tenantId == null) {
            throw new IllegalStateException("No authenticated tenant found in security context");
        }
        
        return tenantId;
    }
    
    /**
     * Validates tenant access for a list of tenant IDs.
     * 
     * Useful when processing batch operations that may span multiple tenants.
     * Ensures the authenticated tenant has access to all requested tenants.
     * 
     * @param requestTenantIds List of tenant IDs to validate
     * @throws TenantAccessDeniedException if any tenant ID doesn't match
     */
    public void validateTenantAccessBatch(Iterable<String> requestTenantIds) {
        if (requestTenantIds == null) {
            throw new IllegalArgumentException("Request tenant IDs must not be null");
        }
        
        for (String tenantId : requestTenantIds) {
            validateTenantAccess(tenantId);
        }
    }
}
