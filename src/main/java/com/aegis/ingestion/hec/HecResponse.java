package com.aegis.ingestion.hec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * HEC Response DTO
 * Response format compatible with Splunk HEC API
 * Follows Splunk HEC response specification
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HecResponse {
    @JsonProperty("text")
    private final String text;
    
    @JsonProperty("code")
    private final int code;
    
    @JsonProperty("ackId")
    private final Long ackId;
    
    @JsonProperty("invalid-event-number")
    private final Integer invalidEventNumber;
    
    /**
     * Create a successful HEC response
     * @param eventCount Number of events processed
     * @param message Success message
     */
    public HecResponse(long eventCount, String message) {
        this(eventCount, message, null);
    }
    
    /**
     * Create a HEC response with optional invalid event number
     * @param eventCount Number of events processed
     * @param message Response message
     * @param invalidEventNumber Index of invalid event (if any)
     */
    public HecResponse(long eventCount, String message, Integer invalidEventNumber) {
        this.text = message;
        this.code = message.startsWith("Error") || message.startsWith("Invalid") ? 1 : 0;
        this.ackId = eventCount > 0 && this.code == 0 ? System.currentTimeMillis() : null;
        this.invalidEventNumber = invalidEventNumber;
    }
    
    /**
     * Create a success response
     * @param eventCount Number of events processed
     * @return HecResponse indicating success
     */
    public static HecResponse success(long eventCount) {
        return new HecResponse(eventCount, "Success");
    }
    
    /**
     * Create an error response
     * @param message Error message
     * @return HecResponse indicating error
     */
    public static HecResponse error(String message) {
        return new HecResponse(0, "Error: " + message);
    }
    
    /**
     * Create an invalid event response
     * @param eventNumber Index of the invalid event
     * @param message Error message
     * @return HecResponse indicating invalid event
     */
    public static HecResponse invalidEvent(int eventNumber, String message) {
        return new HecResponse(0, "Invalid event: " + message, eventNumber);
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
    
    public Integer getInvalidEventNumber() {
        return invalidEventNumber;
    }
    
    @Override
    public String toString() {
        return String.format("HecResponse{text='%s', code=%d, ackId=%d, invalidEventNumber=%d}",
            text, code, ackId, invalidEventNumber);
    }
}
