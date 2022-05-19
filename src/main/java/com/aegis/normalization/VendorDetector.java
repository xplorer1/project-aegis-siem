package com.aegis.normalization;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Detects vendor/log format from raw event data using heuristics.
 * Returns vendor type string for parser selection.
 */
@Component
public class VendorDetector {
    
    /**
     * Detect vendor type from raw event bytes
     */
    public String detectVendor(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return "unknown";
        }
        
        String data = new String(raw, StandardCharsets.UTF_8);
        String trimmed = data.trim();
        
        // Check for CEF format
        if (trimmed.startsWith("CEF:")) {
            return "cef";
        }
        
        // Check for AWS CloudTrail JSON
        if (trimmed.startsWith("{") && data.contains("\"eventVersion\"") && data.contains("\"awsRegion\"")) {
            return "aws:cloudtrail";
        }
        
        // Check for Windows Event Log XML
        if (trimmed.startsWith("<Event") && data.contains("xmlns=\"http://schemas.microsoft.com/win")) {
            return "windows:eventlog";
        }
        
        // Check for Cisco ASA
        if (data.contains("%ASA-")) {
            return "cisco:asa";
        }
        
        // Check for Palo Alto (CSV format with specific fields)
        if (data.contains("TRAFFIC") || data.contains("THREAT") || data.contains("CONFIG")) {
            String[] fields = data.split(",");
            if (fields.length > 20) { // Palo Alto logs have many fields
                return "paloalto:firewall";
            }
        }
        
        // Check for RFC 5424 Syslog
        if (trimmed.matches("<\\d+>\\d+\\s+\\S+.*")) {
            return "syslog:rfc5424";
        }
        
        // Check for RFC 3164 Syslog
        if (trimmed.matches("<\\d+>[A-Za-z]{3}\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2}.*")) {
            return "syslog:rfc3164";
        }
        
        // Check for generic syslog (starts with priority)
        if (trimmed.matches("<\\d+>.*")) {
            return "syslog";
        }
        
        // Check for JSON format (generic)
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            return "json";
        }
        
        // Default to unknown
        return "unknown";
    }
    
    /**
     * Check if data starts with given prefix (case-insensitive)
     */
    private boolean startsWith(byte[] data, String prefix) {
        if (data.length < prefix.length()) {
            return false;
        }
        
        String start = new String(data, 0, prefix.length(), StandardCharsets.UTF_8);
        return start.equalsIgnoreCase(prefix);
    }
    
    /**
     * Check if data contains given substring
     */
    private boolean contains(byte[] data, String substring) {
        String str = new String(data, StandardCharsets.UTF_8);
        return str.contains(substring);
    }
}
