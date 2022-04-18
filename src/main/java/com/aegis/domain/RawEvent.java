package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a raw, unparsed security event as received from ingestion sources.
 * This class stores the original event data along with metadata for processing.
 */
public class RawEvent {
    
    @JsonProperty("data")
    private byte[] data;
    
    @JsonProperty("timestamp")
    private Instant timestamp;
    
    @JsonProperty("tenant_id")
    private String tenantId;
    
    @JsonProperty("vendor_type")
    private String vendorType;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;
    
    /**
     * Default constructor
     */
    public RawEvent() {
        this.metadata = new HashMap<>();
    }
    
    /**
     * Constructor with data and timestamp
     */
    public RawEvent(byte[] data, Instant timestamp) {
        this.data = data;
        this.timestamp = timestamp;
        this.metadata = new HashMap<>();
    }
    
    /**
     * Full constructor
     */
    public RawEvent(byte[] data, Instant timestamp, String tenantId, String vendorType) {
        this.data = data;
        this.timestamp = timestamp;
        this.tenantId = tenantId;
        this.vendorType = vendorType;
        this.metadata = new HashMap<>();
    }
    
    // Getters and Setters
    
    public byte[] getData() {
        return data;
    }
    
    public void setData(byte[] data) {
        this.data = data;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getTenantId() {
        return tenantId;
    }
    
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
    
    public String getVendorType() {
        return vendorType;
    }
    
    public void setVendorType(String vendorType) {
        this.vendorType = vendorType;
    }
    
    public Map<String, Object> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
    
    /**
     * Add a metadata entry
     */
    public void addMetadata(String key, Object value) {
        this.metadata.put(key, value);
    }
}
