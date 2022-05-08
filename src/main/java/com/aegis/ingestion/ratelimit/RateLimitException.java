package com.aegis.ingestion.ratelimit;

/**
 * Exception thrown when a request is rate limited
 */
public class RateLimitException extends RuntimeException {
    
    private final String tenantId;
    
    public RateLimitException(String tenantId) {
        super("Rate limit exceeded for tenant: " + tenantId);
        this.tenantId = tenantId;
    }
    
    public RateLimitException(String tenantId, String message) {
        super(message);
        this.tenantId = tenantId;
    }
    
    public String getTenantId() {
        return tenantId;
    }
}
