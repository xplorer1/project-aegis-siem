package com.aegis.query;

import com.aegis.domain.StorageTier;

/**
 * Exception thrown when query execution fails
 * Provides context about which tier failed and the underlying cause
 */
public class QueryExecutionException extends RuntimeException {
    
    private final StorageTier tier;
    private final String query;
    
    public QueryExecutionException(String message, StorageTier tier) {
        super(message);
        this.tier = tier;
        this.query = null;
    }
    
    public QueryExecutionException(String message, StorageTier tier, Throwable cause) {
        super(message, cause);
        this.tier = tier;
        this.query = null;
    }
    
    public QueryExecutionException(String message, StorageTier tier, String query, Throwable cause) {
        super(message, cause);
        this.tier = tier;
        this.query = query;
    }
    
    public StorageTier getTier() {
        return tier;
    }
    
    public String getQuery() {
        return query;
    }
    
    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder(super.getMessage());
        if (tier != null) {
            sb.append(" [Tier: ").append(tier).append("]");
        }
        if (query != null) {
            sb.append(" [Query: ").append(query).append("]");
        }
        return sb.toString();
    }
}
