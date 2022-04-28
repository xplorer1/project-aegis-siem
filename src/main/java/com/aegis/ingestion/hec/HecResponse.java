package com.aegis.ingestion.hec;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * HEC Response DTO
 * Response format compatible with Splunk HEC API
 */
public class HecResponse {
    @JsonProperty("text")
    private final String text;
    
    @JsonProperty("code")
    private final int code;
    
    @JsonProperty("ackId")
    private final Long ackId;
    
    public HecResponse(long eventCount, String message) {
        this.text = message;
        this.code = message.startsWith("Error") ? 1 : 0;
        this.ackId = eventCount > 0 ? System.currentTimeMillis() : null;
    }
    
    public String getText() {
        return text;
    }
    
    public int getCode() {
        return code;
    }
    
    public Long getAckId() {
        return ackId;
    }
}
