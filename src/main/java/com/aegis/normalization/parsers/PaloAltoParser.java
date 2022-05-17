package com.aegis.normalization.parsers;

import com.aegis.domain.Actor;
import com.aegis.domain.Endpoint;
import com.aegis.domain.OcsfEvent;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Parser for Palo Alto Networks firewall logs.
 * Handles CSV format with comma-separated fields.
 */
public class PaloAltoParser implements EventParser {
    
    private static final DateTimeFormatter DATE_FORMATTER = 
        DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    
    @Override
    public OcsfEvent parse(byte[] raw) throws ParseException {
        try {
            String rawString = new String(raw, StandardCharsets.UTF_8);
            String[] fields = rawString.split(",");
            
            if (fields.length < 10) {
                throw new ParseException("Invalid Palo Alto log format - insufficient fields");
            }
            
            OcsfEvent event = new OcsfEvent();
            event.setUuid(UUID.randomUUID().toString());
            event.setClassUid(4001); // OCSF: Network Activity
            event.setCategoryUid(4); // OCSF: Network Activity
            event.setRawData(rawString);
            
            // Parse fields based on Palo Alto CSV format
            // Field positions may vary by log type, using common traffic log format
            int fieldIndex = 0;
            
            // Skip FUTURE_USE fields (typically first few fields)
            while (fieldIndex < fields.length && fields[fieldIndex].trim().isEmpty()) {
                fieldIndex++;
            }
            
            // Extract timestamp (typically around field 1-2)
            if (fieldIndex < fields.length) {
                try {
                    String timeStr = fields[fieldIndex].trim();
                    if (!timeStr.isEmpty() && timeStr.contains("/")) {
                        LocalDateTime dateTime = LocalDateTime.parse(timeStr, DATE_FORMATTER);
                        event.setTime(dateTime.toInstant(ZoneOffset.UTC));
                    } else {
                        event.setTime(Instant.now());
                    }
                } catch (Exception e) {
                    event.setTime(Instant.now());
                }
            }
            
            // Extract source and destination IPs and ports
            // Typical positions in traffic logs
            if (fields.length > 7) {
                Endpoint srcEndpoint = new Endpoint();
                srcEndpoint.setIp(fields[7].trim()); // Source IP
                if (fields.length > 9 && !fields[9].trim().isEmpty()) {
                    try {
                        srcEndpoint.setPort(Integer.parseInt(fields[9].trim())); // Source port
                    } catch (NumberFormatException e) {
                        // Skip invalid port
                    }
                }
                event.setSrcEndpoint(srcEndpoint);
            }
            
            if (fields.length > 8) {
                Endpoint dstEndpoint = new Endpoint();
                dstEndpoint.setIp(fields[8].trim()); // Destination IP
                if (fields.length > 10 && !fields[10].trim().isEmpty()) {
                    try {
                        dstEndpoint.setPort(Integer.parseInt(fields[10].trim())); // Destination port
                    } catch (NumberFormatException e) {
                        // Skip invalid port
                    }
                }
                event.setDstEndpoint(dstEndpoint);
            }
            
            // Extract action and severity
            String action = fields.length > 30 ? fields[30].trim() : "";
            event.setSeverity(mapActionToSeverity(action));
            
            // Build message
            StringBuilder message = new StringBuilder();
            if (fields.length > 3) {
                message.append("Type: ").append(fields[3].trim());
            }
            if (!action.isEmpty()) {
                message.append(" Action: ").append(action);
            }
            if (fields.length > 28) {
                String app = fields[28].trim();
                if (!app.isEmpty()) {
                    message.append(" App: ").append(app);
                }
            }
            event.setMessage(message.toString());
            
            // Add metadata
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("parser", "paloalto");
            metadata.put("vendor", "paloalto");
            if (!action.isEmpty()) {
                metadata.put("action", action);
            }
            if (fields.length > 4) {
                metadata.put("subtype", fields[4].trim());
            }
            if (fields.length > 28) {
                metadata.put("application", fields[28].trim());
            }
            event.setMetadata(metadata);
            
            return event;
            
        } catch (Exception e) {
            throw new ParseException("Failed to parse Palo Alto event", e);
        }
    }
    
    /**
     * Map Palo Alto action to OCSF severity
     */
    private int mapActionToSeverity(String action) {
        return switch (action.toLowerCase()) {
            case "deny", "drop", "reset-both", "reset-client", "reset-server" -> 4; // High
            case "alert" -> 3; // Medium
            case "allow" -> 1; // Info
            default -> 2; // Low
        };
    }
}
