package com.aegis.query;

import com.aegis.domain.StorageTier;

/**
 * Interface for tier-specific sub-queries
 * Each storage tier (Hot/Warm/Cold) implements this interface
 */
public interface SubQuery {
    
    /**
     * Get the storage tier this sub-query targets
     */
    StorageTier getTier();
    
    /**
     * Get the native query representation for this tier
     * - OpenSearch: SearchSourceBuilder
     * - ClickHouse: SQL String
     * - Iceberg: SQL String
     */
    Object getNativeQuery();
}
