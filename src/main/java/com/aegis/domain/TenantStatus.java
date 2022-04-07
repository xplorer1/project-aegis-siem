package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration of possible tenant statuses in the SIEM system.
 * 
 * The tenant status determines whether the tenant can access the system
 * and what operations are allowed.
 */
public enum TenantStatus {
    
    /**
     * Tenant is active and fully operational.
     * All features and data access are available.
     */
    ACTIVE("active"),
    
    /**
     * Tenant is suspended and cannot access the system.
     * Data is retained but no new events are ingested.
     * Used for non-payment, policy violations, or administrative holds.
     */
    SUSPENDED("suspended"),
    
    /**
     * Tenant is in provisioning state.
     * Resources are being allocated and configured.
     * Limited access until provisioning completes.
     */
    PROVISIONING("provisioning"),
    
    /**
     * Tenant is being decommissioned.
     * Data is being archived or deleted.
     * No access allowed.
     */
    DECOMMISSIONING("decommissioning"),
    
    /**
     * Tenant has been archived.
     * All resources have been released.
     * Data may be retained in cold storage for compliance.
     */
    ARCHIVED("archived");
    
    private final String value;
    
    TenantStatus(String value) {
        this.value = value;
    }
    
    /**
     * Get the string value of the status
     * 
     * @return String representation of the status
     */
    @JsonValue
    public String getValue() {
        return value;
    }
    
    /**
     * Parse a string value to TenantStatus enum
     * 
     * @param value String value to parse
     * @return TenantStatus enum value
     * @throws IllegalArgumentException if value is not recognized
     */
    public static TenantStatus fromValue(String value) {
        for (TenantStatus status : TenantStatus.values()) {
            if (status.value.equalsIgnoreCase(value)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown tenant status: " + value);
    }
    
    /**
     * Check if this status allows data ingestion
     * 
     * @return true if tenant can ingest data in this status
     */
    public boolean allowsIngestion() {
        return this == ACTIVE;
    }
    
    /**
     * Check if this status allows data access
     * 
     * @return true if tenant can access data in this status
     */
    public boolean allowsDataAccess() {
        return this == ACTIVE || this == PROVISIONING;
    }
    
    @Override
    public String toString() {
        return value;
    }
}
