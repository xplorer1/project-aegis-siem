package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a tenant in the multi-tenant SIEM system.
 * 
 * Each tenant represents an isolated customer or organizational unit with
 * their own data, configuration, and resources. Tenant metadata includes
 * configuration settings, provisioning information, and operational status.
 * 
 * Tenants are stored in Redis for fast access during request processing
 * and tenant validation operations.
 */
public class Tenant {
    
    /**
     * Unique identifier for the tenant
     */
    @JsonProperty("id")
    private String id;
    
    /**
     * Human-readable name of the tenant
     */
    @JsonProperty("name")
    private String name;
    
    /**
     * Current status of the tenant
     */
    @JsonProperty("status")
    private TenantStatus status;
    
    /**
     * Timestamp when the tenant was created
     */
    @JsonProperty("created_at")
    private Instant createdAt;
    
    /**
     * Timestamp when the tenant was last updated
     */
    @JsonProperty("updated_at")
    private Instant updatedAt;
    
    /**
     * Tenant tier (e.g., "standard", "premium", "enterprise")
     * Determines resource allocation and feature availability
     */
    @JsonProperty("tier")
    private String tier;
    
    /**
     * Maximum events per second allowed for this tenant
     * Used for rate limiting
     */
    @JsonProperty("max_eps")
    private long maxEps;
    
    /**
     * Data retention period in days
     */
    @JsonProperty("retention_days")
    private int retentionDays;
    
    /**
     * Whether this tenant has dedicated infrastructure
     * Premium tier tenants may have dedicated Kafka topics and indices
     */
    @JsonProperty("dedicated_infrastructure")
    private boolean dedicatedInfrastructure;
    
    /**
     * Tenant-specific configuration settings
     * Stores custom configuration as key-value pairs
     */
    @JsonProperty("settings")
    private Map<String, Object> settings;
    
    /**
     * Contact email for the tenant
     */
    @JsonProperty("contact_email")
    private String contactEmail;
    
    /**
     * Organization name
     */
    @JsonProperty("organization")
    private String organization;
    
    /**
     * Default constructor
     */
    public Tenant() {
        this.settings = new HashMap<>();
        this.status = TenantStatus.ACTIVE;
    }
    
    /**
     * Constructor with required fields
     * 
     * @param id Tenant ID
     * @param name Tenant name
     */
    public Tenant(String id, String name) {
        this();
        this.id = id;
        this.name = name;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }
    
    /**
     * Builder pattern for creating Tenant instances
     */
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private final Tenant tenant;
        
        public Builder() {
            this.tenant = new Tenant();
        }
        
        public Builder id(String id) {
            tenant.id = id;
            return this;
        }
        
        public Builder name(String name) {
            tenant.name = name;
            return this;
        }
        
        public Builder status(TenantStatus status) {
            tenant.status = status;
            return this;
        }
        
        public Builder createdAt(Instant createdAt) {
            tenant.createdAt = createdAt;
            return this;
        }
        
        public Builder updatedAt(Instant updatedAt) {
            tenant.updatedAt = updatedAt;
            return this;
        }
        
        public Builder tier(String tier) {
            tenant.tier = tier;
            return this;
        }
        
        public Builder maxEps(long maxEps) {
            tenant.maxEps = maxEps;
            return this;
        }
        
        public Builder retentionDays(int retentionDays) {
            tenant.retentionDays = retentionDays;
            return this;
        }
        
        public Builder dedicatedInfrastructure(boolean dedicatedInfrastructure) {
            tenant.dedicatedInfrastructure = dedicatedInfrastructure;
            return this;
        }
        
        public Builder settings(Map<String, Object> settings) {
            tenant.settings = settings;
            return this;
        }
        
        public Builder contactEmail(String contactEmail) {
            tenant.contactEmail = contactEmail;
            return this;
        }
        
        public Builder organization(String organization) {
            tenant.organization = organization;
            return this;
        }
        
        public Tenant build() {
            if (tenant.createdAt == null) {
                tenant.createdAt = Instant.now();
            }
            if (tenant.updatedAt == null) {
                tenant.updatedAt = Instant.now();
            }
            return tenant;
        }
    }
    
    // Getters and Setters
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public TenantStatus getStatus() {
        return status;
    }
    
    public void setStatus(TenantStatus status) {
        this.status = status;
    }
    
    public Instant getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
    
    public Instant getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public String getTier() {
        return tier;
    }
    
    public void setTier(String tier) {
        this.tier = tier;
    }
    
    public long getMaxEps() {
        return maxEps;
    }
    
    public void setMaxEps(long maxEps) {
        this.maxEps = maxEps;
    }
    
    public int getRetentionDays() {
        return retentionDays;
    }
    
    public void setRetentionDays(int retentionDays) {
        this.retentionDays = retentionDays;
    }
    
    public boolean isDedicatedInfrastructure() {
        return dedicatedInfrastructure;
    }
    
    public void setDedicatedInfrastructure(boolean dedicatedInfrastructure) {
        this.dedicatedInfrastructure = dedicatedInfrastructure;
    }
    
    public Map<String, Object> getSettings() {
        return settings;
    }
    
    public void setSettings(Map<String, Object> settings) {
        this.settings = settings;
    }
    
    public String getContactEmail() {
        return contactEmail;
    }
    
    public void setContactEmail(String contactEmail) {
        this.contactEmail = contactEmail;
    }
    
    public String getOrganization() {
        return organization;
    }
    
    public void setOrganization(String organization) {
        this.organization = organization;
    }
    
    /**
     * Check if tenant is active
     * 
     * @return true if tenant status is ACTIVE
     */
    public boolean isActive() {
        return status == TenantStatus.ACTIVE;
    }
    
    /**
     * Check if tenant is suspended
     * 
     * @return true if tenant status is SUSPENDED
     */
    public boolean isSuspended() {
        return status == TenantStatus.SUSPENDED;
    }
    
    /**
     * Update the updatedAt timestamp to current time
     */
    public void touch() {
        this.updatedAt = Instant.now();
    }
    
    @Override
    public String toString() {
        return "Tenant{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", status=" + status +
                ", tier='" + tier + '\'' +
                ", organization='" + organization + '\'' +
                '}';
    }
}
