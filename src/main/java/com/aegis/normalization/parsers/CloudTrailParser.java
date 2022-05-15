package com.aegis.normalization.parsers;

import com.aegis.domain.Actor;
import com.aegis.domain.Endpoint;
import com.aegis.domain.OcsfEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Parser for AWS CloudTrail JSON events.
 * Maps CloudTrail format to OCSF schema.
 */
public class CloudTrailParser implements EventParser {
    
    private final ObjectMapper mapper;
    
    public CloudTrailParser() {
        this.mapper = new ObjectMapper()
            .registerModule(new AfterburnerModule()); // Bytecode generation for performance
    }
    
    @Override
    public OcsfEvent parse(byte[] raw) throws ParseException {
        try {
            JsonNode json = mapper.readTree(raw);
            String rawString = new String(raw, StandardCharsets.UTF_8);
            
            OcsfEvent event = new OcsfEvent();
            event.setUuid(UUID.randomUUID().toString());
            event.setTime(parseTime(json));
            event.setSeverity(mapSeverity(json));
            event.setClassUid(3005); // OCSF: API Activity
            event.setCategoryUid(3); // OCSF: System Activity
            event.setActor(extractActor(json));
            event.setSrcEndpoint(extractSourceEndpoint(json));
            event.setMessage(extractMessage(json));
            event.setRawData(rawString);
            event.setMetadata(extractMetadata(json));
            
            return event;
            
        } catch (Exception e) {
            throw new ParseException("Failed to parse CloudTrail event", e);
        }
    }
    
    /**
     * Parse CloudTrail eventTime field
     */
    private Instant parseTime(JsonNode json) {
        if (json.has("eventTime")) {
            return Instant.parse(json.get("eventTime").asText());
        }
        return Instant.now();
    }
    
    /**
     * Map CloudTrail error codes to OCSF severity
     */
    private int mapSeverity(JsonNode json) {
        if (json.has("errorCode")) {
            String errorCode = json.get("errorCode").asText();
            // Errors are higher severity
            if (errorCode.contains("AccessDenied") || errorCode.contains("Unauthorized")) {
                return 4; // High
            }
            return 3; // Medium
        }
        
        // Check for sensitive operations
        if (json.has("eventName")) {
            String eventName = json.get("eventName").asText();
            if (eventName.contains("Delete") || eventName.contains("Terminate") || 
                eventName.contains("Modify") || eventName.contains("Update")) {
                return 2; // Low
            }
        }
        
        return 1; // Info
    }
    
    /**
     * Extract actor (user identity) from CloudTrail event
     */
    private Actor extractActor(JsonNode json) {
        Actor actor = new Actor();
        
        if (json.has("userIdentity")) {
            JsonNode userIdentity = json.get("userIdentity");
            
            // Extract user name
            if (userIdentity.has("userName")) {
                actor.setUser(userIdentity.get("userName").asText());
            } else if (userIdentity.has("principalId")) {
                actor.setUser(userIdentity.get("principalId").asText());
            } else if (userIdentity.has("arn")) {
                actor.setUser(userIdentity.get("arn").asText());
            }
            
            // Extract session context
            if (userIdentity.has("sessionContext")) {
                JsonNode sessionContext = userIdentity.get("sessionContext");
                if (sessionContext.has("sessionIssuer")) {
                    JsonNode sessionIssuer = sessionContext.get("sessionIssuer");
                    if (sessionIssuer.has("userName")) {
                        actor.setSession(sessionIssuer.get("userName").asText());
                    }
                }
            }
        }
        
        // Extract user agent as process
        if (json.has("userAgent")) {
            actor.setProcess(json.get("userAgent").asText());
        }
        
        return actor;
    }
    
    /**
     * Extract source endpoint (IP address)
     */
    private Endpoint extractSourceEndpoint(JsonNode json) {
        Endpoint endpoint = new Endpoint();
        
        if (json.has("sourceIPAddress")) {
            endpoint.setIp(json.get("sourceIPAddress").asText());
        }
        
        return endpoint;
    }
    
    /**
     * Extract message from CloudTrail event
     */
    private String extractMessage(JsonNode json) {
        StringBuilder message = new StringBuilder();
        
        if (json.has("eventName")) {
            message.append(json.get("eventName").asText());
        }
        
        if (json.has("eventSource")) {
            message.append(" on ").append(json.get("eventSource").asText());
        }
        
        if (json.has("errorCode")) {
            message.append(" - Error: ").append(json.get("errorCode").asText());
        }
        
        if (json.has("errorMessage")) {
            message.append(" (").append(json.get("errorMessage").asText()).append(")");
        }
        
        return message.toString();
    }
    
    /**
     * Extract metadata from CloudTrail event
     */
    private Map<String, Object> extractMetadata(JsonNode json) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("parser", "cloudtrail");
        metadata.put("vendor", "aws");
        
        if (json.has("eventID")) {
            metadata.put("eventId", json.get("eventID").asText());
        }
        
        if (json.has("eventType")) {
            metadata.put("eventType", json.get("eventType").asText());
        }
        
        if (json.has("awsRegion")) {
            metadata.put("region", json.get("awsRegion").asText());
        }
        
        if (json.has("recipientAccountId")) {
            metadata.put("accountId", json.get("recipientAccountId").asText());
        }
        
        if (json.has("requestID")) {
            metadata.put("requestId", json.get("requestID").asText());
        }
        
        if (json.has("resources")) {
            JsonNode resources = json.get("resources");
            if (resources.isArray() && resources.size() > 0) {
                metadata.put("resourceCount", resources.size());
            }
        }
        
        return metadata;
    }
}
