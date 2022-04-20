package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents threat intelligence information enriched from external sources.
 * This class contains reputation scores and threat indicators for IPs, domains, and other IOCs.
 */
public class ThreatInfo {
    
    /**
     * Constant representing safe/benign threat info
     */
    public static final ThreatInfo SAFE = new ThreatInfo(0, "safe", new ArrayList<>());
    
    @JsonProperty("reputation_score")
    private int reputationScore; // 0-100, where 0 is safe and 100 is malicious
    
    @JsonProperty("threat_category")
    private String threatCategory; // e.g., "malware", "phishing", "botnet", "safe"
    
    @JsonProperty("threat_indicators")
    private List<String> threatIndicators; // List of associated threat indicators
    
    @JsonProperty("confidence")
    private Double confidence; // Confidence level 0.0-1.0
    
    @JsonProperty("source")
    private String source; // Threat intelligence source
    
    /**
     * Default constructor
     */
    public ThreatInfo() {
        this.threatIndicators = new ArrayList<>();
    }
    
    /**
     * Constructor with basic fields
     */
    public ThreatInfo(int reputationScore, String threatCategory, List<String> threatIndicators) {
        this.reputationScore = reputationScore;
        this.threatCategory = threatCategory;
        this.threatIndicators = threatIndicators != null ? threatIndicators : new ArrayList<>();
    }
    
    /**
     * Full constructor
     */
    public ThreatInfo(int reputationScore, String threatCategory, List<String> threatIndicators, 
                      Double confidence, String source) {
        this.reputationScore = reputationScore;
        this.threatCategory = threatCategory;
        this.threatIndicators = threatIndicators != null ? threatIndicators : new ArrayList<>();
        this.confidence = confidence;
        this.source = source;
    }
    
    // Getters and Setters
    
    public int getReputationScore() {
        return reputationScore;
    }
    
    public void setReputationScore(int reputationScore) {
        this.reputationScore = reputationScore;
    }
    
    public String getThreatCategory() {
        return threatCategory;
    }
    
    public void setThreatCategory(String threatCategory) {
        this.threatCategory = threatCategory;
    }
    
    public List<String> getThreatIndicators() {
        return threatIndicators;
    }
    
    public void setThreatIndicators(List<String> threatIndicators) {
        this.threatIndicators = threatIndicators;
    }
    
    public Double getConfidence() {
        return confidence;
    }
    
    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    /**
     * Add a threat indicator
     */
    public void addThreatIndicator(String indicator) {
        this.threatIndicators.add(indicator);
    }
}
