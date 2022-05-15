package com.aegis.normalization.parsers;

import com.aegis.domain.OcsfEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Generic parser for unknown or unstructured log formats.
 * Attempts basic parsing and creates a minimal OCSF event.
 * Falls back to storing the entire raw data as message field.
 */
public class GenericParser implements EventParser {
    
    private static final Logger log = LoggerFactory.getLogger(GenericParser.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public OcsfEvent parse(byte[] raw) throws ParseException {
        try {
            String rawString = new String(raw, StandardCharsets.UTF_8);
            
            // Try to detect if it's JSON
            if (rawString.trim().startsWith("{")) {
                return parseJson(rawString, raw);
            }
            
            // Otherwise, create a basic event with raw data
            return parseUnknown(rawString, raw);
            
        } catch (Exception e) {
            throw new ParseException("Failed to parse event with generic parser", e);
        }
    }
    
    /**
     * Parse JSON-formatted events
     */
    private OcsfEvent parseJson(String rawString, byte[] raw) throws Exception {
        JsonNode json = objectMapper.readTree(rawString);
        
        OcsfEvent event = new OcsfEvent();
        event.setUuid(UUID.randomUUID().toString());
        event.setTime(extractTimestamp(json));
        event.setSeverity(extractSeverity(json));
        event.setClassUid(1000); // Generic: Unknown
        event.setCategoryUid(0); // Unknown category
        event.setMessage(extractMessage(json));
        event.setRawData(rawString);
        event.setMetadata(extractMetadata(json));
        
        return event;
    }
    
    /**
     * Parse unknown format - create minimal event
     */
    private OcsfEvent parseUnknown(String rawString, byte[] raw) {
        OcsfEvent event = new OcsfEvent();
        event.setUuid(UUID.randomUUID().toString());
        event.setTime(Instant.now()); // Use current time as fallback
        event.setSeverity(1); // Default to Info
        event.setClassUid(1000); // Generic: Unknown
        event.setCategoryUid(0); // Unknown category
        event.setMessage(rawString.length() > 1000 ? 
            rawString.substring(0, 1000) + "..." : rawString);
        event.setRawData(rawString);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("parser", "generic");
        metadata.put("format", "unknown");
        event.setMetadata(metadata);
        
        return event;
    }
    
    /**
     * Extract timestamp from JSON, trying common field names
     */
    private Instant extractTimestamp(JsonNode json) {
        // Try common timestamp field names
        String[] timestampFields = {"timestamp", "time", "@timestamp", "eventTime", "datetime", "date"};
        
        for (String field : timestampFields) {
            if (json.has(field)) {
                try {
                    String timeStr = json.get(field).asText();
                    return Instant.parse(timeStr);
                } catch (Exception e) {
                    log.debug("Failed to parse timestamp from field: {}", field);
                }
            }
        }
        
        // Fallback to current time
        return Instant.now();
    }
    
    /**
     * Extract severity from JSON, trying common field names
     */
    private int extractSeverity(JsonNode json) {
        String[] severityFields = {"severity", "level", "priority", "sev"};
        
        for (String field : severityFields) {
            if (json.has(field)) {
                String severityStr = json.get(field).asText().toLowerCase();
                return mapSeverity(severityStr);
            }
        }
        
        return 1; // Default to Info
    }
    
    /**
     * Map various severity strings to OCSF 1-5 scale
     */
    private int mapSeverity(String severity) {
        return switch (severity) {
            case "critical", "fatal", "emergency" -> 5;
            case "high", "error", "alert" -> 4;
            case "medium", "warning", "warn" -> 3;
            case "low", "notice" -> 2;
            default -> 1; // info, debug, trace
        };
    }
    
    /**
     * Extract message from JSON
     */
    private String extractMessage(JsonNode json) {
        String[] messageFields = {"message", "msg", "description", "text", "event"};
        
        for (String field : messageFields) {
            if (json.has(field)) {
                return json.get(field).asText();
            }
        }
        
        // Fallback to entire JSON as string
        return json.toString();
    }
    
    /**
     * Extract metadata from JSON
     */
    private Map<String, Object> extractMetadata(JsonNode json) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("parser", "generic");
        metadata.put("format", "json");
        
        // Add all top-level fields as metadata
        json.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            
            if (value.isTextual()) {
                metadata.put(key, value.asText());
            } else if (value.isNumber()) {
                metadata.put(key, value.numberValue());
            } else if (value.isBoolean()) {
                metadata.put(key, value.asBoolean());
            }
        });
        
        return metadata;
    }
}
