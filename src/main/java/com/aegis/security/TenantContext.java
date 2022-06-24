package com.aegis.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-local context for storing the current tenant ID.
 * 
 * This class provides a thread-safe mechanism for storing and retrieving
 * the tenant ID for the current request context. It uses ThreadLocal to
 * ensure that each thread has its own isolated tenant context.
 * 
 * The tenant ID is used throughout the application to enforce multi-tenant
 * isolation at the database driver level, ensuring that queries and data
 * access operations are automatically scoped to the current tenant.
 * 
 * Usage:
 * <pre>
 * // Set tenant ID at the beginning of request processing
 * TenantContext.setTenantId("tenant-123");
 * 
 * // Retrieve tenant ID when needed
 * String tenantId = TenantContext.getTenantId();
 * 
 * // Clear tenant ID at the end of request processing
 * TenantContext.clear();
 * </pre>
 * 
 * @see ThreadLocal
 */
public class TenantContext {
    
    private static final Logger log = LoggerFactory.getLogger(TenantContext.class);
    
    /**
     * ThreadLocal storage for the current tenant ID.
     * Each thread maintains its own tenant context to ensure isolation
     * between concurrent requests.
     */
    private static final ThreadLocal<String> TENANT_ID = new ThreadLocal<>();
    
    /**
     * Private constructor to prevent instantiation.
     * This is a utility class with only static methods.
     */
    private TenantContext() {
        throw new UnsupportedOperationException("TenantContext is a utility class and cannot be instantiated");
    }
    
    /**
     * Sets the tenant ID for the current thread.
     * 
     * This method should be called at the beginning of request processing,
     * typically by an interceptor or filter that extracts the tenant ID
     * from the authentication token or request headers.
     * 
     * @param tenantId the tenant ID to set, must not be null or empty
     * @throws IllegalArgumentException if tenantId is null or empty
     */
    public static void setTenantId(String tenantId) {
        if (tenantId == null || tenantId.trim().isEmpty()) {
            throw new IllegalArgumentException("Tenant ID must not be null or empty");
        }
        
        log.debug("Setting tenant ID: {}", tenantId);
        TENANT_ID.set(tenantId);
    }
    
    /**
     * Retrieves the tenant ID for the current thread.
     * 
     * This method returns the tenant ID that was previously set using
     * {@link #setTenantId(String)}. If no tenant ID has been set for
     * the current thread, this method returns null.
     * 
     * @return the tenant ID for the current thread, or null if not set
     */
    public static String getTenantId() {
        String tenantId = TENANT_ID.get();
        log.trace("Retrieved tenant ID: {}", tenantId);
        return tenantId;
    }
    
    /**
     * Clears the tenant ID for the current thread.
     * 
     * This method should be called at the end of request processing to
     * prevent memory leaks and ensure that the ThreadLocal storage is
     * properly cleaned up. It is typically called in a finally block
     * or by a filter/interceptor.
     * 
     * Failure to call this method can result in:
     * - Memory leaks in thread pool environments
     * - Tenant ID bleeding between requests when threads are reused
     */
    public static void clear() {
        String tenantId = TENANT_ID.get();
        if (tenantId != null) {
            log.debug("Clearing tenant ID: {}", tenantId);
        }
        TENANT_ID.remove();
    }
    
    /**
     * Checks if a tenant ID is currently set for the current thread.
     * 
     * @return true if a tenant ID is set, false otherwise
     */
    public static boolean isSet() {
        return TENANT_ID.get() != null;
    }
    
    /**
     * Gets the tenant ID for the current thread, throwing an exception
     * if no tenant ID is set.
     * 
     * This method is useful when a tenant ID is required and the absence
     * of one indicates a programming error or security violation.
     * 
     * @return the tenant ID for the current thread
     * @throws IllegalStateException if no tenant ID is set
     */
    public static String requireTenantId() {
        String tenantId = getTenantId();
        if (tenantId == null) {
            throw new IllegalStateException("No tenant ID set in current context");
        }
        return tenantId;
    }
}
