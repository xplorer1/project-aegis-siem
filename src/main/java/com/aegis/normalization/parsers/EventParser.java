package com.aegis.normalization.parsers;

import com.aegis.domain.OcsfEvent;

/**
 * Interface for parsing vendor-specific log formats to OCSF schema
 * Implementations should handle specific vendor formats (AWS CloudTrail, Cisco ASA, etc.)
 */
public interface EventParser {
    
    /**
     * Parses raw event bytes into an OCSF-compliant event
     * 
     * @param raw the raw event data as byte array
     * @return parsed OcsfEvent
     * @throws ParseException if parsing fails
     */
    OcsfEvent parse(byte[] raw) throws ParseException;
    
    /**
     * Returns the vendor type this parser handles
     * 
     * @return vendor type identifier (e.g., "aws:cloudtrail", "cisco:asa")
     */
    String getVendorType();
}
