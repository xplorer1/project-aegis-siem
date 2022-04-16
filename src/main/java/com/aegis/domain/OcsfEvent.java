package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

/**
 * Base class for OCSF (Open Cybersecurity Schema Framework) events.
 * This class represents the core structure for all security events in the AEGIS SIEM system.
 */
@Document(indexName = "aegis-events-#{T(java.time.LocalDate).now()}")
public class OcsfEvent {
    
    @Id
    @JsonProperty("uuid")
    private String uuid;
    
    /**
     * Default constructor for Jackson deserialization
     */
    public OcsfEvent() {
    }
    
    /**
     * Constructor with UUID
     */
    public OcsfEvent(String uuid) {
        this.uuid = uuid;
    }
    
    // Getters and Setters
    
    public String getUuid() {
        return uuid;
    }
    
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
}
