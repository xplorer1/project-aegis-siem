package com.aegis.ingestion.hec;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * HEC Health Response DTO
 */
public class HecHealthResponse {
    @JsonProperty("status")
    private final String status;
    
    public HecHealthResponse(String status) {
        this.status = status;
    }
    
    public String getStatus() {
        return status;
    }
}
