package com.aegis.tenant;

import com.aegis.domain.TenantStatus;

import java.util.Map;

/**
 * Request model for tenant provisioning.
 */
public class TenantProvisioningRequest {
    private String id;
    private String name;
    private String organization;
    private String contactEmail;

    private String tier;
    private Long maxEps;
    private Integer retentionDays;
    private Boolean dedicatedInfrastructure;

    private Map<String, Object> settings;

    /**
     * Whether to create dedicated Kafka topics for this tenant.
     * If not provided, defaults based on {@link #dedicatedInfrastructure}.
     */
    private Boolean provisionKafkaTopics;

    /**
     * Initial status to set during provisioning.
     * Defaults to {@link TenantStatus#PROVISIONING}.
     */
    private TenantStatus initialStatus;

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

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public String getContactEmail() {
        return contactEmail;
    }

    public void setContactEmail(String contactEmail) {
        this.contactEmail = contactEmail;
    }

    public String getTier() {
        return tier;
    }

    public void setTier(String tier) {
        this.tier = tier;
    }

    public Long getMaxEps() {
        return maxEps;
    }

    public void setMaxEps(Long maxEps) {
        this.maxEps = maxEps;
    }

    public Integer getRetentionDays() {
        return retentionDays;
    }

    public void setRetentionDays(Integer retentionDays) {
        this.retentionDays = retentionDays;
    }

    public Boolean getDedicatedInfrastructure() {
        return dedicatedInfrastructure;
    }

    public void setDedicatedInfrastructure(Boolean dedicatedInfrastructure) {
        this.dedicatedInfrastructure = dedicatedInfrastructure;
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, Object> settings) {
        this.settings = settings;
    }

    public Boolean getProvisionKafkaTopics() {
        return provisionKafkaTopics;
    }

    public void setProvisionKafkaTopics(Boolean provisionKafkaTopics) {
        this.provisionKafkaTopics = provisionKafkaTopics;
    }

    public TenantStatus getInitialStatus() {
        return initialStatus;
    }

    public void setInitialStatus(TenantStatus initialStatus) {
        this.initialStatus = initialStatus;
    }
}

