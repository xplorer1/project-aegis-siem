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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for RFC 5424 Syslog format.
 * Handles structured syslog messages with optional structured data.
 */
public class SyslogParser implements EventParser {
    
    // RFC 5424 pattern: <priority>version timestamp hostname app-name procid msgid [structured-data] message
    private static final Pattern RFC5424_PATTERN = Pattern.compile(
        "<(\\d+)>(\\d+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\[.*?\\]|-)\\s*(.*)"
    );
    
    // RFC 3164 pattern (older format): <priority>timestamp hostname tag: message
    private static final Pattern RFC3164_PATTERN = Pattern.compile(
        "<(\\d+)>([A-Za-z]{3}\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2})\\s+(\\S+)\\s+(\\S+):\\s*(.*)"
    );
    
    // Structured data pattern: [id param="value" ...]
    private static final Pattern STRUCTURED_DATA_PATTERN = Pattern.compile(
        "\\[(\\S+)\\s+([^\\]]+)\\]"
    );
    
    @Override
    public OcsfEvent parse(byte[] raw) throws ParseException {
        try {
            String rawString = new String(raw, StandardCharsets.UTF_8);
            
            // Try RFC 5424 first
            Matcher rfc5424Matcher = RFC5424_PATTERN.matcher(rawString);
            if (rfc5424Matcher.matches()) {
                return parseRfc5424(rfc5424Matcher, rawString);
            }
            
            // Try RFC 3164
            Matcher rfc3164Matcher = RFC3164_PATTERN.matcher(rawString);
            if (rfc3164Matcher.matches()) {
                return parseRfc3164(rfc3164Matcher, rawString);
            }
            
            // Fallback to generic parsing
            return parseGenericSyslog(rawString);
            
        } catch (Exception e) {
            throw new ParseException("Failed to parse Syslog event", e);
        }
    }
    
    /**
     * Parse RFC 5424 format syslog
     */
    private OcsfEvent parseRfc5424(Matcher matcher, String rawString) {
        OcsfEvent event = new OcsfEvent();
        event.setUuid(UUID.randomUUID().toString());
        event.setClassUid(6001); // OCSF: System Activity
        event.setCategoryUid(6); // OCSF: System Activity
        event.setRawData(rawString);
        
        // Extract priority and calculate severity
        int priority = Integer.parseInt(matcher.group(1));
        int severity = priority & 0x07; // Last 3 bits
        int facility = priority >> 3;   // First bits
        event.setSeverity(mapSyslogSeverity(severity));
        
        // Extract timestamp
        String timestamp = matcher.group(3);
        try {
            event.setTime(Instant.parse(timestamp));
        } catch (Exception e) {
            event.setTime(Instant.now());
        }
        
        // Extract hostname
        String hostname = matcher.group(4);
        if (!hostname.equals("-")) {
            Endpoint srcEndpoint = new Endpoint();
            srcEndpoint.setHostname(hostname);
            event.setSrcEndpoint(srcEndpoint);
        }
        
        // Extract app-name and procid
        String appName = matcher.group(5);
        String procId = matcher.group(6);
        if (!appName.equals("-") || !procId.equals("-")) {
            Actor actor = new Actor();
            if (!appName.equals("-")) {
                actor.setProcess(appName);
            }
            if (!procId.equals("-")) {
                actor.setSession(procId);
            }
            event.setActor(actor);
        }
        
        // Extract message
        String message = matcher.group(9);
        event.setMessage(message);
        
        // Extract structured data
        String structuredData = matcher.group(8);
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("parser", "syslog");
        metadata.put("format", "rfc5424");
        metadata.put("facility", facility);
        metadata.put("msgid", matcher.group(7));
        
        if (!structuredData.equals("-")) {
            parseStructuredData(structuredData, metadata);
        }
        
        event.setMetadata(metadata);
        
        return event;
    }
    
    /**
     * Parse RFC 3164 format syslog
     */
    private OcsfEvent parseRfc3164(Matcher matcher, String rawString) {
        OcsfEvent event = new OcsfEvent();
        event.setUuid(UUID.randomUUID().toString());
        event.setClassUid(6001); // OCSF: System Activity
        event.setCategoryUid(6); // OCSF: System Activity
        event.setRawData(rawString);
        
        // Extract priority
        int priority = Integer.parseInt(matcher.group(1));
        int severity = priority & 0x07;
        int facility = priority >> 3;
        event.setSeverity(mapSyslogSeverity(severity));
        
        // Extract timestamp (RFC 3164 format: "MMM dd HH:mm:ss")
        String timestamp = matcher.group(2);
        try {
            // Add current year since RFC 3164 doesn't include it
            int currentYear = LocalDateTime.now().getYear();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMM d HH:mm:ss yyyy");
            LocalDateTime dateTime = LocalDateTime.parse(timestamp + " " + currentYear, formatter);
            event.setTime(dateTime.toInstant(ZoneOffset.UTC));
        } catch (Exception e) {
            event.setTime(Instant.now());
        }
        
        // Extract hostname
        String hostname = matcher.group(3);
        Endpoint srcEndpoint = new Endpoint();
        srcEndpoint.setHostname(hostname);
        event.setSrcEndpoint(srcEndpoint);
        
        // Extract tag (app name)
        String tag = matcher.group(4);
        Actor actor = new Actor();
        actor.setProcess(tag);
        event.setActor(actor);
        
        // Extract message
        String message = matcher.group(5);
        event.setMessage(message);
        
        // Add metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("parser", "syslog");
        metadata.put("format", "rfc3164");
        metadata.put("facility", facility);
        event.setMetadata(metadata);
        
        return event;
    }
    
    /**
     * Parse generic syslog (fallback)
     */
    private OcsfEvent parseGenericSyslog(String rawString) {
        OcsfEvent event = new OcsfEvent();
        event.setUuid(UUID.randomUUID().toString());
        event.setTime(Instant.now());
        event.setSeverity(1);
        event.setClassUid(6001);
        event.setCategoryUid(6);
        event.setMessage(rawString);
        event.setRawData(rawString);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("parser", "syslog");
        metadata.put("format", "generic");
        event.setMetadata(metadata);
        
        return event;
    }
    
    /**
     * Parse structured data section
     */
    private void parseStructuredData(String structuredData, Map<String, Object> metadata) {
        Matcher matcher = STRUCTURED_DATA_PATTERN.matcher(structuredData);
        while (matcher.find()) {
            String id = matcher.group(1);
            String params = matcher.group(2);
            
            // Parse key="value" pairs
            Pattern paramPattern = Pattern.compile("(\\S+)=\"([^\"]*)\"");
            Matcher paramMatcher = paramPattern.matcher(params);
            while (paramMatcher.find()) {
                String key = id + "." + paramMatcher.group(1);
                String value = paramMatcher.group(2);
                metadata.put(key, value);
            }
        }
    }
    
    /**
     * Map syslog severity (0-7) to OCSF severity (1-5)
     */
    private int mapSyslogSeverity(int syslogSeverity) {
        return switch (syslogSeverity) {
            case 0, 1 -> 5; // Emergency, Alert -> Critical
            case 2, 3 -> 4; // Critical, Error -> High
            case 4 -> 3;    // Warning -> Medium
            case 5 -> 2;    // Notice -> Low
            default -> 1;   // Info, Debug -> Info
        };
    }
}
