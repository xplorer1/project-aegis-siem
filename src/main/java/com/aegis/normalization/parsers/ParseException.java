package com.aegis.normalization.parsers;

/**
 * Exception thrown when event parsing fails
 * Contains error context to help diagnose parsing issues
 */
public class ParseException extends RuntimeException {
    
    private final String vendorType;
    private final String rawData;
    
    public ParseException(String message) {
        super(message);
        this.vendorType = null;
        this.rawData = null;
    }
    
    public ParseException(String message, Throwable cause) {
        super(message, cause);
        this.vendorType = null;
        this.rawData = null;
    }
    
    public ParseException(String message, String vendorType, String rawData) {
        super(message);
        this.vendorType = vendorType;
        this.rawData = rawData;
    }
    
    public ParseException(String message, Throwable cause, String vendorType, String rawData) {
        super(message, cause);
        this.vendorType = vendorType;
        this.rawData = rawData;
    }
    
    public String getVendorType() {
        return vendorType;
    }
    
    public String getRawData() {
        return rawData;
    }
}
