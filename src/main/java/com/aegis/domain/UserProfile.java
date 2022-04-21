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
