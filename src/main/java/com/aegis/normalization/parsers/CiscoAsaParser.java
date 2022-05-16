package com.aegis.normalization.parsers;

import com.aegis.domain.Actor;
import com.aegis.domain.Endpoint;
import com.aegis.domain.OcsfEvent;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for Cisco ASA firewall syslog messages.
 * Parses syslog format and extracts network connection details.
 */
public class CiscoAsaParser implements EventParser {
    
    // Cisco ASA syslog pattern: %ASA-severity-messageId: message
    private static final Pattern ASA_PATTERN = Pattern.compile(
        "%ASA-(\\d)-(\\d+):\\s*(.+)"
    );
    
    // Pattern for extracting IP addresses and ports
    private static final Pattern IP_PORT_PATTERN = Pattern.compile(
        "(\\d+\\.\\d+\\.\\d+\\.\\d+)(?:/(\\d+))?"
    );
    
    // Pattern for connection messages
    private static final Pattern CONNECTION_PATTERN = Pattern.compile(
        "(Built|Teardown)\\s+(\\w+)\\s+connection\\s+\\d+\\s+for\\s+(\\w+):(\\S+)/(\\d+)\\s+\\((\\S+)/(\\d+)\\)\\s+to\\s+(\\w+):(\\S+)/(\\d+)\\s+\\((\\S+)/(\\d+)\\)"
    );
    
    @Override
    public OcsfEvent parse(byte[] raw) throws ParseException {
        try {
            String rawString = new String(raw, StandardCharsets.UTF_8);
            
            OcsfEvent event = new OcsfEvent();
            event.setUuid(UUID.randomUUID().toString());
            event.setTime(Instant.now()); // ASA logs don't always have timestamps
            event.setClassUid(4001); // OCSF: Network Activity
            event.setCategoryUid(4); // OCSF: Network Activity
            event.setRawData(rawString);
            
            // Parse ASA message
            Matcher asaMatcher = ASA_PATTERN.matcher(rawString);
            if (asaMatcher.find()) {
                int severity = Integer.parseInt(asaMatcher.group(1));
                String messageId = asaMatcher.group(2);
                String message = asaMatcher.group(3);
                
                event.setSeverity(mapAsaSeverity(severity));
                event.setMessage(message);
                
                // Extract connection details if present
                extractConnectionDetails(event, message);
                
                // Add metadata
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("parser", "cisco-asa");
                metadata.put("vendor", "cisco");
                metadata.put("messageId", messageId);
                event.setMetadata(metadata);
            } else {
                // Fallback for non-standard format
                event.setSeverity(1);
                event.setMessage(rawString);
                
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("parser", "cisco-asa");
                metadata.put("vendor", "cisco");
                event.setMetadata(metadata);
            }
            
            return event;
            
        } catch (Exception e) {
            throw new ParseException("Failed to parse Cisco ASA event", e);
        }
    }
    
    /**
     * Map Cisco ASA severity (0-7) to OCSF severity (1-5)
     */
    private int mapAsaSeverity(int asaSeverity) {
        return switch (asaSeverity) {
            case 0, 1 -> 5; // Emergency, Alert -> Critical
            case 2, 3 -> 4; // Critical, Error -> High
            case 4 -> 3;    // Warning -> Medium
            case 5 -> 2;    // Notification -> Low
            default -> 1;   // Informational, Debug -> Info
        };
    }
    
    /**
     * Extract connection details from message
     */
    private void extractConnectionDetails(OcsfEvent event, String message) {
        Matcher connMatcher = CONNECTION_PATTERN.matcher(message);
        if (connMatcher.find()) {
            String action = connMatcher.group(1); // Built or Teardown
            String protocol = connMatcher.group(2);
            
            // Source endpoint
            Endpoint srcEndpoint = new Endpoint();
            srcEndpoint.setIp(connMatcher.group(6)); // Translated source IP
            srcEndpoint.setPort(Integer.parseInt(connMatcher.group(7))); // Translated source port
            event.setSrcEndpoint(srcEndpoint);
            
            // Destination endpoint
            Endpoint dstEndpoint = new Endpoint();
            dstEndpoint.setIp(connMatcher.group(11)); // Translated dest IP
            dstEndpoint.setPort(Integer.parseInt(connMatcher.group(12))); // Translated dest port
            event.setDstEndpoint(dstEndpoint);
            
            // Add protocol to metadata
            if (event.getMetadata() == null) {
                event.setMetadata(new HashMap<>());
            }
            event.getMetadata().put("protocol", protocol);
            event.getMetadata().put("action", action.toLowerCase());
        }
    }
}
