package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a failed event that could not be parsed or processed.
 * Dead letter events are stored for debugging and manual reprocessing.
 */
public class DeadLetter {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("raw_event")
    private RawEvent rawEvent;
    
    @JsonProperty("error_message")
    private String errorMessage;
    
    @JsonProperty("error_type")
    private String errorType;
    
    @JsonProperty("stack_trace")
    private String stackTrace;
    
    @JsonProperty("failed_at")
    private Instant failedAt;
    
    @JsonProperty("retry_count")
    private int retryCount;
    
    @JsonProperty("error_context")
    private Map<String, Object> errorContext;
    
    /**
     * Default constructor
     */
    public DeadLetter() {
        this.errorContext = new HashMap<>();
        this.failedAt = Instant.now();
        this.retryCount = 0;
    }
    
    /**
     * Constructor with raw event and error message
     */
    public DeadLetter(RawEvent rawEvent, String errorMessage) {
        this.rawEvent = rawEvent;
        this.errorMessage = errorMessage;
        this.errorContext = new HashMap<>();
        this.failedAt = Instant.now();
        this.retryCount = 0;
    }
    
    /**
     * Full constructor
     */
    public DeadLetter(String id, RawEvent rawEvent, String errorMessage, String errorType, 
                      String stackTrace, Instant failedAt, int retryCount) {
        this.id = id;
        this.rawEvent = rawEvent;
        this.errorMessage = errorMessage;
        this.errorType = errorType;
        this.stackTrace = stackTrace;
        this.failedAt = failedAt;
        this.retryCount = retryCount;
        this.errorContext = new HashMap<>();
    }
    
    // Getters and Setters
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public RawEvent getRawEvent() {
        return rawEvent;
    }
    
    public void setRawEvent(RawEvent rawEvent) {
        this.rawEvent = rawEvent;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    public String getErrorType() {
        return errorType;
    }
    
    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }
    
    public String getStackTrace() {
        return stackTrace;
    }
    
    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }
    
    public Instant getFailedAt() {
        return failedAt;
    }
    
    public void setFailedAt(Instant failedAt) {
        this.failedAt = failedAt;
    }
    
    public int getRetryCount() {
        return retryCount;
    }
    
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }
    
    public Map<String, Object> getErrorContext() {
        return errorContext;
    }
    
    public void setErrorContext(Map<String, Object> errorContext) {
        this.errorContext = errorContext;
    }
    
    /**
     * Add error context
     */
    public void addErrorContext(String key, Object value) {
        this.errorContext.put(key, value);
    }
    
    /**
     * Increment retry count
     */
    public void incrementRetryCount() {
        this.retryCount++;
    }
}
