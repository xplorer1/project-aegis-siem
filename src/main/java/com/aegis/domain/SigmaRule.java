package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.util.List;
import java.util.Map;

/**
 * Represents a Sigma detection rule for threat correlation.
 * Sigma rules are YAML-based detection rules that define patterns for identifying security threats.
 */
public class SigmaRule {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("title")
    private String title;
    
    @JsonProperty("status")
    private String status; // e.g., "stable", "experimental", "deprecated"
    
    @JsonProperty("description")
    private String description;
    
    @JsonProperty("author")
    private String author;
    
    @JsonProperty("date")
    private String date;
    
    @JsonProperty("modified")
    private String modified;
    
    @JsonProperty("logsource")
    private Map<String, Object> logsource;
    
    @JsonProperty("detection")
    private Map<String, Object> detection;
    
    @JsonProperty("falsepositives")
    private List<String> falsepositives;
    
    @JsonProperty("level")
    private String level; // e.g., "low", "medium", "high", "critical"
    
    @JsonProperty("tags")
    private List<String> tags;
    
    /**
     * Default constructor
     */
    public SigmaRule() {
    }
    
    /**
     * Constructor with basic fields
     */
    public SigmaRule(String id, String title, String status) {
        this.id = id;
        this.title = title;
        this.status = status;
    }
    
    /**
     * Parse a Sigma rule from YAML string
     */
    public static SigmaRule fromYaml(String yaml) throws Exception {
        YAMLMapper mapper = new YAMLMapper();
        return mapper.readValue(yaml, SigmaRule.class);
    }
    
    // Getters and Setters
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getTitle() {
        return title;
    }
    
    public void setTitle(String title) {
        this.title = title;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getAuthor() {
        return author;
    }
    
    public void setAuthor(String author) {
        this.author = author;
    }
    
    public String getDate() {
        return date;
    }
    
    public void setDate(String date) {
        this.date = date;
    }
    
    public String getModified() {
        return modified;
    }
    
    public void setModified(String modified) {
        this.modified = modified;
    }
    
    public Map<String, Object> getLogsource() {
        return logsource;
    }
    
    public void setLogsource(Map<String, Object> logsource) {
        this.logsource = logsource;
    }
    
    public Map<String, Object> getDetection() {
        return detection;
    }
    
    public void setDetection(Map<String, Object> detection) {
        this.detection = detection;
    }
    
    public List<String> getFalsepositives() {
        return falsepositives;
    }
    
    public void setFalsepositives(List<String> falsepositives) {
        this.falsepositives = falsepositives;
    }
    
    public String getLevel() {
        return level;
    }
    
    public void setLevel(String level) {
        this.level = level;
    }
    
    public List<String> getTags() {
        return tags;
    }
    
    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}
