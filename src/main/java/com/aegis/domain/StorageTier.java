package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration representing the storage tiers in the AEGIS SIEM system.
 * Data is automatically migrated between tiers based on age and access patterns.
 */
public enum StorageTier {
    
    /**
     * Hot tier: 0-7 days, full-text search capability (OpenSearch)
     */
    HOT("hot", 0, 7, "OpenSearch with full-text search"),
    
    /**
     * Warm tier: 7-90 days, optimized for aggregations (ClickHouse)
     */
    WARM("warm", 7, 90, "ClickHouse optimized for OLAP"),
    
    /**
     * Cold tier: 90 days - 7 years, columnar format (Apache Iceberg)
     */
    COLD("cold", 90, 2555, "Apache Iceberg on object storage");
    
    private final String value;
    private final int retentionStartDays;
    private final int retentionEndDays;
    private final String description;
    
    StorageTier(String value, int retentionStartDays, int retentionEndDays, String description) {
        this.value = value;
        this.retentionStartDays = retentionStartDays;
        this.retentionEndDays = retentionEndDays;
        this.description = description;
    }
    
    @JsonValue
    public String getValue() {
        return value;
    }
    
    public int getRetentionStartDays() {
        return retentionStartDays;
    }
    
    public int getRetentionEndDays() {
        return retentionEndDays;
    }
    
    public String getDescription() {
        return description;
    }
    
    /**
     * Parse a string value to StorageTier
     */
    public static StorageTier fromValue(String value) {
        for (StorageTier tier : StorageTier.values()) {
            if (tier.value.equalsIgnoreCase(value)) {
                return tier;
            }
        }
        throw new IllegalArgumentException("Unknown StorageTier value: " + value);
    }
    
    /**
     * Determine the appropriate storage tier based on data age in days
     */
    public static StorageTier fromAge(int ageDays) {
        if (ageDays < HOT.retentionEndDays) {
            return HOT;
        } else if (ageDays < WARM.retentionEndDays) {
            return WARM;
        } else {
            return COLD;
        }
    }
    
    /**
     * Check if data should be migrated to the next tier
     */
    public boolean shouldMigrate(int ageDays) {
        return ageDays >= this.retentionEndDays;
    }
}
