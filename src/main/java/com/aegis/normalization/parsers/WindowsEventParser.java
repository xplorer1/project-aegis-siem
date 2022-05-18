package com.aegis.normalization.parsers;

import com.aegis.domain.Actor;
import com.aegis.domain.Endpoint;
import com.aegis.domain.OcsfEvent;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Parser for Windows Event Log XML format.
 * Handles Windows Event Log forwarded via WinRM or other mechanisms.
 */
public class WindowsEventParser implements EventParser {
    
    private final DocumentBuilderFactory factory;
    
    public WindowsEventParser() {
        this.factory = DocumentBuilderFactory.newInstance();
        this.factory.setNamespaceAware(true);
    }
    
    @Override
    public OcsfEvent parse(byte[] raw) throws ParseException {
        try {
            String rawString = new String(raw, StandardCharsets.UTF_8);
            
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(raw));
            
            OcsfEvent event = new OcsfEvent();
            event.setUuid(UUID.randomUUID().toString());
            event.setClassUid(3001); // OCSF: Process Activity
            event.setCategoryUid(3); // OCSF: System Activity
            event.setRawData(rawString);
            
            // Extract System section
            Element system = (Element) doc.getElementsByTagName("System").item(0);
            if (system != null) {
                // Extract EventID
                String eventId = getElementText(system, "EventID");
                
                // Extract TimeCreated
                Element timeCreated = (Element) system.getElementsByTagName("TimeCreated").item(0);
                if (timeCreated != null) {
                    String systemTime = timeCreated.getAttribute("SystemTime");
                    if (!systemTime.isEmpty()) {
                        event.setTime(ZonedDateTime.parse(systemTime, DateTimeFormatter.ISO_DATE_TIME).toInstant());
                    } else {
                        event.setTime(Instant.now());
                    }
                } else {
                    event.setTime(Instant.now());
                }
                
                // Extract Level (severity)
                String level = getElementText(system, "Level");
                event.setSeverity(mapWindowsLevel(level));
                
                // Extract Computer
                String computer = getElementText(system, "Computer");
                if (!computer.isEmpty()) {
                    Endpoint srcEndpoint = new Endpoint();
                    srcEndpoint.setHostname(computer);
                    event.setSrcEndpoint(srcEndpoint);
                }
                
                // Add to metadata
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("parser", "windows-event");
                metadata.put("vendor", "microsoft");
                metadata.put("eventId", eventId);
                metadata.put("channel", getElementText(system, "Channel"));
                metadata.put("provider", getElementText(system, "Provider"));
                event.setMetadata(metadata);
            }
            
            // Extract EventData section
            Element eventData = (Element) doc.getElementsByTagName("EventData").item(0);
            if (eventData != null) {
                Actor actor = new Actor();
                
                // Extract user information
                NodeList dataNodes = eventData.getElementsByTagName("Data");
                for (int i = 0; i < dataNodes.getLength(); i++) {
                    Element dataElement = (Element) dataNodes.item(i);
                    String name = dataElement.getAttribute("Name");
                    String value = dataElement.getTextContent();
                    
                    switch (name) {
                        case "SubjectUserName", "TargetUserName" -> actor.setUser(value);
                        case "ProcessName", "NewProcessName" -> actor.setProcess(value);
                        case "LogonId" -> actor.setSession(value);
                    }
                    
                    // Add all data to metadata
                    if (event.getMetadata() != null && !value.isEmpty()) {
                        event.getMetadata().put(name, value);
                    }
                }
                
                if (actor.getUser() != null || actor.getProcess() != null) {
                    event.setActor(actor);
                }
            }
            
            // Build message
            String eventId = event.getMetadata() != null ? 
                (String) event.getMetadata().get("eventId") : "Unknown";
            String channel = event.getMetadata() != null ? 
                (String) event.getMetadata().get("channel") : "Unknown";
            event.setMessage(String.format("Windows Event %s from %s", eventId, channel));
            
            return event;
            
        } catch (Exception e) {
            throw new ParseException("Failed to parse Windows Event Log", e);
        }
    }
    
    /**
     * Get text content of first matching element
     */
    private String getElementText(Element parent, String tagName) {
        NodeList nodes = parent.getElementsByTagName(tagName);
        if (nodes.getLength() > 0) {
            Element element = (Element) nodes.item(0);
            return element.getTextContent();
        }
        return "";
    }
    
    /**
     * Map Windows event level to OCSF severity
     */
    private int mapWindowsLevel(String level) {
        if (level.isEmpty()) {
            return 1;
        }
        
        try {
            int levelNum = Integer.parseInt(level);
            return switch (levelNum) {
                case 1 -> 5; // Critical
                case 2 -> 4; // Error -> High
                case 3 -> 3; // Warning -> Medium
                case 4 -> 1; // Information -> Info
                default -> 2; // Verbose -> Low
            };
        } catch (NumberFormatException e) {
            return 1;
        }
    }
}
