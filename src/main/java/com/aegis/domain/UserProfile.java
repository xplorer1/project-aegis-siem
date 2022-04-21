package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;

import java.time.Instant;

/**
 * Represents a user behavior profile for UEBA (User and Entity Behavior Analytics).
 * This class stores behavioral baselines and patterns for anomaly detection.
 * Stored in Redis with a 30-day TTL.
 */
@RedisHash("user-profile")
public class UserProfile {
    
    @Id
    @JsonProperty("user_id")
    private String userId;
    
    @JsonProperty("tenant_id")
    private String tenantId;
    
    // Login patterns
    @JsonProperty("avg_login_hour")
    private double avgLoginHour;
    
    @JsonProperty("std_dev_login_hour")
    private double stdDevLoginHour;
    
    @JsonProperty("typical_countries")
    private java.util.Set<String> typicalCountries;
    
    @JsonProperty("typical_ip_ranges")
    private java.util.Set<String> typicalIpRanges;
    
    // Data access patterns
    @JsonProperty("avg_data_volume")
    private double avgDataVolume;
    
    @JsonProperty("std_dev_data_volume")
    private double stdDevDataVolume;
    
    @JsonProperty("typical_resources")
    private java.util.Set<String> typicalResources;
    
    @JsonProperty("baseline_start")
    private Instant baselineStart;
    
    @JsonProperty("baseline_end")
    private Instant baselineEnd;
    
    @JsonProperty("event_count")
    private long eventCount;
    
    @TimeToLive
    @JsonProperty("ttl")
    private Long ttl = 2592000L; // 30 days in seconds
    
    /**
     * Default constructor
     */
    public UserProfile() {
    }
    
    /**
     * Constructor with user ID
     */
    public UserProfile(String userId) {
        this.userId = userId;
    }
    
    /**
     * Constructor with user ID and tenant ID
     */
    public UserProfile(String userId, String tenantId) {
        this.userId = userId;
        this.tenantId = tenantId;
        this.typicalCountries = new java.util.HashSet<>();
        this.typicalIpRanges = new java.util.HashSet<>();
        this.typicalResources = new java.util.HashSet<>();
    }
    
    // Getters and Setters
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getTenantId() {
        return tenantId;
    }
    
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
    
    public double getAvgLoginHour() {
        return avgLoginHour;
    }
    
    public void setAvgLoginHour(double avgLoginHour) {
        this.avgLoginHour = avgLoginHour;
    }
    
    public double getStdDevLoginHour() {
        return stdDevLoginHour;
    }
    
    public void setStdDevLoginHour(double stdDevLoginHour) {
        this.stdDevLoginHour = stdDevLoginHour;
    }
    
    public java.util.Set<String> getTypicalCountries() {
        return typicalCountries;
    }
    
    public void setTypicalCountries(java.util.Set<String> typicalCountries) {
        this.typicalCountries = typicalCountries;
    }
    
    public java.util.Set<String> getTypicalIpRanges() {
        return typicalIpRanges;
    }
    
    public void setTypicalIpRanges(java.util.Set<String> typicalIpRanges) {
        this.typicalIpRanges = typicalIpRanges;
    }
    
    public double getAvgDataVolume() {
        return avgDataVolume;
    }
    
    public void setAvgDataVolume(double avgDataVolume) {
        this.avgDataVolume = avgDataVolume;
    }
    
    public double getStdDevDataVolume() {
        return stdDevDataVolume;
    }
    
    public void setStdDevDataVolume(double stdDevDataVolume) {
        this.stdDevDataVolume = stdDevDataVolume;
    }
    
    public java.util.Set<String> getTypicalResources() {
        return typicalResources;
    }
    
    public void setTypicalResources(java.util.Set<String> typicalResources) {
        this.typicalResources = typicalResources;
    }
    
    public Instant getBaselineStart() {
        return baselineStart;
    }
    
    public void setBaselineStart(Instant baselineStart) {
        this.baselineStart = baselineStart;
    }
    
    public Instant getBaselineEnd() {
        return baselineEnd;
    }
    
    public void setBaselineEnd(Instant baselineEnd) {
        this.baselineEnd = baselineEnd;
    }
    
    public long getEventCount() {
        return eventCount;
    }
    
    public void setEventCount(long eventCount) {
        this.eventCount = eventCount;
    }
    
    public Long getTtl() {
        return ttl;
    }
    
    public void setTtl(Long ttl) {
        this.ttl = ttl;
    }
}
