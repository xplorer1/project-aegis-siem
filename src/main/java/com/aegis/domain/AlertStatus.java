package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration representing the lifecycle status of a security alert.
 * Alerts progress through these states as they are investigated and resolved.
 */
public enum AlertStatus {
    
    /**
     * Alert has been created but not yet reviewed
     */
    OPEN("open"),
    
    /**
     * Alert has been acknowledged by a security analyst
     */
    ACKNOWLEDGED("acknowledged"),
    
    /**
     * Alert is currently being investigated
     */
    IN_PROGRESS("in_progress"),
    
    /**
     * Alert has been resolved
     */
    RESOLVED("resolved"),
    
    /**
     * Alert has been determined to be a false positive
     */
    FALSE_POSITIVE("false_positive");
    
    private final String value;
    
    AlertStatus(String value) {
        this.value = value;
    }
    
    @JsonValue
    public String getValue() {
        return value;
    }
    
    /**
     * Parse a string value to AlertStatus
     */
    public static AlertStatus fromValue(String value) {
        for (AlertStatus status : AlertStatus.values()) {
            if (status.value.equalsIgnoreCase(value)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown AlertStatus value: " + value);
    }
}
