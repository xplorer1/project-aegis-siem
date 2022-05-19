package com.aegis.normalization.parsers;

import com.aegis.domain.Actor;
import com.aegis.domain.Endpoint;
import com.aegis.domain.OcsfEvent;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for Common Event Format (CEF).
 * Format: CEF:Version|Device Vendor|Device Product|Device Version|Signature ID|Name|Severity|Extension
 */
public class CefParser implements EventParser {
    
    // CEF header pattern
    private static final Pattern CEF_PATTERN = Pattern.compile(
        "CEF:(\\d+)\\|([^|]*)\\|([^|]*)\\|([^|]*)\\|([^|]*)\\|([^|]*)\\|([^|]*)\\|(.*)"
    );
    
    // Extension key-value pattern
    private static final Pattern EXTENSION_PATTERN = Pattern.compile(
        "(\\w+)=([^=]+?)(?=\\s+\\w+=|$)"
    );
    
    @Override
    public OcsfEvent parse(byte[] raw) throws ParseException {
        try {
            String rawString = new String(raw, StandardCharsets.UTF_8);
            
            Matcher matcher = CEF_PATTERN.matcher(rawString);
            if (!matcher.matches()) {
                throw new ParseException("Invalid CEF format");
            }
            
            OcsfEvent event = new OcsfEvent();
            event.setUuid(UUID.randomUUID().toString());
            event.setClassUid(9999); // Generic - will be refined based on signature
            event.setCategoryUid(99); // Unknown
            event.setRawData(rawString);
            
            // Extract CEF header fields
            String version = matcher.group(1);
            String deviceVendor = matcher.group(2);
            String deviceProduct = matcher.group(3);
            String deviceVersion = matcher.group(4);
            String signatureId = matcher.group(5);
            String name = matcher.group(6);
            String severity = matcher.group(7);
            String extension = matcher.group(8);
            
            // Map severity
            event.setSeverity(mapCefSeverity(severity));
            
            // Set message
            event.setMessage(name);
            
            // Parse extensions
            Map<String, String> extensions = parseExtensions(extension);
            
            // Extract timestamp
            if (extensions.containsKey("rt")) {
                try {
                    long timestamp = Long.parseLong(extensions.get("rt"));
                    event.setTime(Instant.ofEpochMilli(timestamp));
                } catch (NumberFormatException e) {
                    event.setTime(Instant.now());
                }
            } else if (extensions.containsKey("end")) {
                try {
                    long timestamp = Long.parseLong(extensions.get("end"));
                    event.setTime(Instant.ofEpochMilli(timestamp));
                } catch (NumberFormatException e) {
                    event.setTime(Instant.now());
                }
            } else {
                event.setTime(Instant.now());
            }
            
            // Extract actor
            Actor actor = new Actor();
            if (extensions.containsKey("suser")) {
                actor.setUser(extensions.get("suser"));
            } else if (extensions.containsKey("duser")) {
                actor.setUser(extensions.get("duser"));
            }
            if (extensions.containsKey("sproc")) {
                actor.setProcess(extensions.get("sproc"));
            }
            if (actor.getUser() != null || actor.getProcess() != null) {
                event.setActor(actor);
            }
            
            // Extract source endpoint
            Endpoint srcEndpoint = new Endpoint();
            if (extensions.containsKey("src")) {
                srcEndpoint.setIp(extensions.get("src"));
            }
            if (extensions.containsKey("spt")) {
                try {
                    srcEndpoint.setPort(Integer.parseInt(extensions.get("spt")));
                } catch (NumberFormatException e) {
                    // Skip invalid port
                }
            }
            if (extensions.containsKey("shost")) {
                srcEndpoint.setHostname(extensions.get("shost"));
            }
            if (extensions.containsKey("smac")) {
                srcEndpoint.setMac(extensions.get("smac"));
            }
            if (srcEndpoint.getIp() != null || srcEndpoint.getHostname() != null) {
                event.setSrcEndpoint(srcEndpoint);
            }
            
            // Extract destination endpoint
            Endpoint dstEndpoint = new Endpoint();
            if (extensions.containsKey("dst")) {
                dstEndpoint.setIp(extensions.get("dst"));
            }
            if (extensions.containsKey("dpt")) {
                try {
                    dstEndpoint.setPort(Integer.parseInt(extensions.get("dpt")));
                } catch (NumberFormatException e) {
                    // Skip invalid port
                }
            }
            if (extensions.containsKey("dhost")) {
                dstEndpoint.setHostname(extensions.get("dhost"));
            }
            if (extensions.containsKey("dmac")) {
                dstEndpoint.setMac(extensions.get("dmac"));
            }
            if (dstEndpoint.getIp() != null || dstEndpoint.getHostname() != null) {
                event.setDstEndpoint(dstEndpoint);
            }
            
            // Build metadata
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("parser", "cef");
            metadata.put("cefVersion", version);
            metadata.put("deviceVendor", deviceVendor);
            metadata.put("deviceProduct", deviceProduct);
            metadata.put("deviceVersion", deviceVersion);
            metadata.put("signatureId", signatureId);
            
            // Add all extensions to metadata
            extensions.forEach((key, value) -> {
                if (!value.isEmpty()) {
                    metadata.put("cef." + key, value);
                }
            });
            
            event.setMetadata(metadata);
            
            return event;
            
        } catch (Exception e) {
            throw new ParseException("Failed to parse CEF event", e);
        }
    }
    
    /**
     * Parse CEF extension key-value pairs
     */
    private Map<String, String> parseExtensions(String extension) {
        Map<String, String> extensions = new HashMap<>();
        
        Matcher matcher = EXTENSION_PATTERN.matcher(extension);
        while (matcher.find()) {
            String key = matcher.group(1);
            String value = matcher.group(2).trim();
            extensions.put(key, value);
        }
        
        return extensions;
    }
    
    /**
     * Map CEF severity (0-10) to OCSF severity (1-5)
     */
    private int mapCefSeverity(String severity) {
        try {
            int sev = Integer.parseInt(severity);
            if (sev >= 8) return 5;  // Critical
            if (sev >= 6) return 4;  // High
            if (sev >= 4) return 3;  // Medium
            if (sev >= 2) return 2;  // Low
            return 1;                // Info
        } catch (NumberFormatException e) {
            return 1;
        }
    }
}
