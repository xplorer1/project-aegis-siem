package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;

/**
 * Base class for OCSF (Open Cybersecurity Schema Framework) events.
 * This class represents the core structure for all security events in the AEGIS SIEM system.
 */
@Document(indexName = "aegis-events-#{T(java.time.LocalDate).now()}")
public class OcsfEvent {
    
    @Id
    @JsonProperty("uuid")
    private String uuid;
    
    @Field(type = FieldType.Date)
    @JsonProperty("time")
    private Instant time;
    
    @Field(type = FieldType.Keyword)
    @JsonProperty("tenant_id")
    private String tenantId;
    
    @Field(type = FieldType.Byte)
    @JsonProperty("severity")
    private int severity; // 1=Info, 2=Low, 3=Medium, 4=High, 5=Critical
    
    @Field(type = FieldType.Short)
    @JsonProperty("class_uid")
    private int classUid; // OCSF class identifier
    
    @Field(type = FieldType.Byte)
    @JsonProperty("category_uid")
    private int categoryUid; // OCSF category identifier
    
    @Field(type = FieldType.Object)
    @JsonProperty("threat_info")
    private ThreatInfo threatInfo;
    
    @Field(type = FieldType.Double)
    @JsonProperty("ueba_score")
    private Double uebaScore;
    
    @Field(type = FieldType.Object)
    @JsonProperty("metadata")
    private java.util.Map<String, Object> metadata;
    
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
    
    public Instant getTime() {
        return time;
    }
    
    public void setTime(Instant time) {
        this.time = time;
    }
    
    public String getTenantId() {
        return tenantId;
    }
    
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
    
    public int getSeverity() {
        return severity;
    }
    
    public void setSeverity(int severity) {
        this.severity = severity;
    }
    
    public int getClassUid() {
        return classUid;
    }
    
    public void setClassUid(int classUid) {
        this.classUid = classUid;
    }
    
    public int getCategoryUid() {
        return categoryUid;
    }
    
    public void setCategoryUid(int categoryUid) {
        this.categoryUid = categoryUid;
    }
    
    public ThreatInfo getThreatInfo() {
        return threatInfo;
    }
    
    public void setThreatInfo(ThreatInfo threatInfo) {
        this.threatInfo = threatInfo;
    }
    
    public Double getUebaScore() {
        return uebaScore;
    }
    
    public void setUebaScore(Double uebaScore) {
        this.uebaScore = uebaScore;
    }
    
    public java.util.Map<String, Object> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(java.util.Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
