package com.aegis.correlation.rules;

import com.aegis.domain.Alert;
import com.aegis.domain.AlertStatus;
import com.aegis.domain.OcsfEvent;
import com.aegis.domain.SigmaRule;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Compiles Sigma detection rules into executable Flink stream processing topologies.
 * 
 * Sigma rules are YAML-based threat detection rules that define patterns for identifying
 * security threats. This compiler parses Sigma rules and converts them into Flink operators
 * that can process event streams in real-time.
 * 
 * The compiler supports:
 * - Selection criteria matching (field-based filtering)
 * - Temporal windowing (tumbling, sliding, session windows)
 * - Aggregation functions (count, sum, etc.)
 * - Threshold-based detection
 * 
 * Example Sigma rule:
 * <pre>
 * title: Brute Force Attack
 * status: stable
 * logsource:
 *   category: authentication
 * detection:
 *   selection:
 *     event_type: authentication
 *     outcome: failure
 *   condition: selection | count(source_ip) by user > 5 in 5m
 *   timeframe: 5m
 * </pre>
 */
@Component
public class SigmaRuleCompiler {
    
    private static final Logger log = LoggerFactory.getLogger(SigmaRuleCompiler.class);
    
    private final YAMLMapper yamlMapper;
    
    /**
     * Constructor initializes the YAML mapper for parsing Sigma rules.
     */
    public SigmaRuleCompiler() {
        this.yamlMapper = new YAMLMapper();
    }
    
    /**
     * Parses a Sigma rule from YAML string format.
     * 
     * This method uses the SnakeYAML library (via Jackson's YAMLMapper) to parse
     * YAML-formatted Sigma rules into SigmaRule objects. The detection section
     * is parsed as a nested map structure containing selection criteria, conditions,
     * and timeframes.
     * 
     * Example YAML:
     * <pre>
     * title: Brute Force Attack
     * status: stable
     * logsource:
     *   category: authentication
     * detection:
     *   selection:
     *     event_type: authentication
     *     outcome: failure
     *   condition: selection | count(source_ip) by user > 5 in 5m
     *   timeframe: 5m
     * level: high
     * </pre>
     * 
     * @param yaml the YAML string containing the Sigma rule
     * @return the parsed SigmaRule object
     * @throws IOException if the YAML cannot be parsed
     */
    public SigmaRule parseYamlRule(String yaml) throws IOException {
        log.debug("Parsing Sigma rule from YAML");
        
        try {
            SigmaRule rule = yamlMapper.readValue(yaml, SigmaRule.class);
            
            // Validate that the rule has required fields
            if (rule.getTitle() == null || rule.getTitle().isEmpty()) {
                throw new IOException("Sigma rule must have a title");
            }
            
            if (rule.getDetection() == null || rule.getDetection().isEmpty()) {
                throw new IOException("Sigma rule must have a detection section");
            }
            
            log.info("Successfully parsed Sigma rule: {}", rule.getTitle());
            return rule;
            
        } catch (IOException e) {
            log.error("Failed to parse Sigma rule from YAML: {}", e.getMessage());
            throw e;
        }
    }
    
    /**
     * Parses the detection section of a Sigma rule.
     * 
     * The detection section contains:
     * - selection: field-value pairs that events must match
     * - condition: the logical condition and aggregation to apply
     * - timeframe: the time window for aggregation
     * 
     * @param rule the Sigma rule containing the detection section
     * @return a map containing the parsed detection configuration
     */
    public Map<String, Object> parseDetectionSection(SigmaRule rule) {
        Map<String, Object> detection = rule.getDetection();
        
        if (detection == null) {
            log.warn("Rule {} has no detection section", rule.getId());
            return new HashMap<>();
        }
        
        log.debug("Parsing detection section for rule: {}", rule.getTitle());
        log.debug("Detection keys: {}", detection.keySet());
        
        // Log the parsed components for debugging
        if (detection.containsKey("selection")) {
            log.debug("Selection criteria: {}", detection.get("selection"));
        }
        if (detection.containsKey("condition")) {
            log.debug("Condition: {}", detection.get("condition"));
        }
        if (detection.containsKey("timeframe")) {
            log.debug("Timeframe: {}", detection.get("timeframe"));
        }
        
        return detection;
    }
    
    /**
     * Compiles a Sigma rule into a Flink DataStream transformation.
     * 
     * The compilation process:
     * 1. Filters events based on the rule's selection criteria
     * 2. Keys the stream by the specified grouping field (e.g., user)
     * 3. Applies time-based windowing (tumbling, sliding, or session)
     * 4. Aggregates events within each window
     * 5. Filters aggregates that exceed the threshold
     * 6. Maps matching aggregates to Alert objects
     * 
     * @param rule the Sigma rule to compile
     * @param events the input stream of OCSF events
     * @return a DataStream of alerts generated when the rule matches
     */
    public DataStream<Alert> compile(SigmaRule rule, DataStream<OcsfEvent> events) {
        log.info("Compiling Sigma rule: {} (ID: {})", rule.getTitle(), rule.getId());
        
        // Extract detection configuration
        Map<String, Object> detection = rule.getDetection();
        if (detection == null || detection.isEmpty()) {
            log.warn("Rule {} has no detection configuration", rule.getId());
            return events.filter(e -> false).map(e -> null); // Empty stream
        }
        
        // Extract selection criteria
        @SuppressWarnings("unchecked")
        Map<String, Object> selection = (Map<String, Object>) detection.get("selection");
        
        // Extract condition and timeframe
        String condition = (String) detection.get("condition");
        Object timeframeObj = detection.get("timeframe");
        
        // Parse timeframe (e.g., "5m" -> 5 minutes)
        long timeframeMinutes = parseTimeframe(timeframeObj);
        
        // Parse threshold from condition (e.g., "> 5")
        int threshold = parseThreshold(condition);
        
        // Parse grouping key from condition (e.g., "by user")
        String groupByField = parseGroupByField(condition);
        
        // Step 1: Filter events based on selection criteria
        DataStream<OcsfEvent> filtered = events.filter(event -> matchesSelection(event, selection));
        
        // Step 2: Key by grouping field (e.g., user, source IP)
        // For simplicity, we'll use tenantId as the key
        // In a full implementation, this would be dynamic based on groupByField
        DataStream<OcsfEvent> keyed = filtered.keyBy(OcsfEvent::getTenantId);
        
        // Step 3: Apply windowing based on timeframe
        // Default to tumbling windows for simplicity
        DataStream<Map<String, Long>> aggregated = keyed
            .window(TumblingEventTimeWindows.of(Time.minutes(timeframeMinutes)))
            .aggregate(new CountBySourceIp());
        
        // Step 4: Filter aggregates that exceed threshold
        DataStream<Map<String, Long>> thresholdExceeded = aggregated
            .filter(counts -> {
                for (Long count : counts.values()) {
                    if (count > threshold) {
                        return true;
                    }
                }
                return false;
            });
        
        // Step 5: Create alerts from matching aggregates
        return thresholdExceeded.map(counts -> createAlert(rule, counts));
    }
    
    /**
     * Checks if an event matches the selection criteria defined in a Sigma rule.
     * 
     * The selection criteria is a map of field names to expected values.
     * This method checks if the event's fields match all the criteria.
     * 
     * Supported field mappings:
     * - event_type, class_uid -> OcsfEvent.classUid
     * - category, category_uid -> OcsfEvent.categoryUid
     * - severity -> OcsfEvent.severity
     * - tenant_id -> OcsfEvent.tenantId
     * - outcome -> extracted from event metadata
     * - user -> extracted from actor
     * - source_ip, src_ip -> extracted from source endpoint
     * - dest_ip, dst_ip -> extracted from destination endpoint
     * 
     * @param event the OCSF event to check
     * @param selection the selection criteria from the Sigma rule
     * @return true if the event matches all selection criteria, false otherwise
     */
    public boolean matchesSelection(OcsfEvent event, Map<String, Object> selection) {
        if (selection == null || selection.isEmpty()) {
            return true; // No selection criteria means match all
        }
        
        log.trace("Checking event against selection criteria: {}", selection);
        
        // Check each selection criterion
        for (Map.Entry<String, Object> criterion : selection.entrySet()) {
            String field = criterion.getKey();
            Object expectedValue = criterion.getValue();
            
            // Get the actual value from the event
            Object actualValue = getFieldValue(event, field);
            
            // Check if values match
            if (!valuesMatch(actualValue, expectedValue)) {
                log.trace("Event does not match criterion: {} = {} (expected: {})", 
                    field, actualValue, expectedValue);
                return false;
            }
        }
        
        log.trace("Event matches all selection criteria");
        return true;
    }
    
    /**
     * Extracts a field value from an OCSF event.
     * 
     * This implementation handles common Sigma field names and maps them to
     * OCSF event fields. It supports nested field access for actor and endpoint data.
     * 
     * Supported fields:
     * - event_type, class_uid: event classification
     * - category, category_uid: event category
     * - severity: event severity level
     * - tenant_id: tenant identifier
     * - outcome: event outcome (success/failure)
     * - user: actor username
     * - process: actor process name
     * - source_ip, src_ip: source IP address
     * - dest_ip, dst_ip: destination IP address
     * - source_port, src_port: source port number
     * - dest_port, dst_port: destination port number
     * 
     * @param event the OCSF event
     * @param fieldName the name of the field to extract
     * @return the field value, or null if not found
     */
    private Object getFieldValue(OcsfEvent event, String fieldName) {
        if (event == null || fieldName == null) {
            return null;
        }
        
        // Normalize field name to lowercase for case-insensitive matching
        String normalizedField = fieldName.toLowerCase().trim();
        
        // Map common Sigma field names to OCSF event fields
        return switch (normalizedField) {
            case "event_type", "class_uid" -> event.getClassUid();
            case "category", "category_uid" -> event.getCategoryUid();
            case "severity" -> event.getSeverity();
            case "tenant_id" -> event.getTenantId();
            
            // Actor-related fields
            case "user", "username" -> event.getActor() != null ? event.getActor().getUser() : null;
            case "process", "process_name" -> event.getActor() != null ? event.getActor().getProcess() : null;
            
            // Source endpoint fields
            case "source_ip", "src_ip", "sourceip" -> 
                event.getSrcEndpoint() != null ? event.getSrcEndpoint().getIp() : null;
            case "source_port", "src_port", "sourceport" -> 
                event.getSrcEndpoint() != null ? event.getSrcEndpoint().getPort() : null;
            case "source_hostname", "src_hostname" -> 
                event.getSrcEndpoint() != null ? event.getSrcEndpoint().getHostname() : null;
            
            // Destination endpoint fields
            case "dest_ip", "dst_ip", "destination_ip", "destip" -> 
                event.getDstEndpoint() != null ? event.getDstEndpoint().getIp() : null;
            case "dest_port", "dst_port", "destination_port", "destport" -> 
                event.getDstEndpoint() != null ? event.getDstEndpoint().getPort() : null;
            case "dest_hostname", "dst_hostname" -> 
                event.getDstEndpoint() != null ? event.getDstEndpoint().getHostname() : null;
            
            // Metadata fields
            case "outcome", "result" -> event.getMetadata() != null ? 
                event.getMetadata().get("outcome") : null;
            case "message" -> event.getMessage();
            
            default -> {
                log.trace("Unknown field: {}", fieldName);
                yield null; // Field not found
            }
        };
    }
    
    /**
     * Checks if two values match, handling different types and comparison operators.
     * 
     * This method supports:
     * - Exact string matching (case-insensitive)
     * - Numeric equality
     * - Null handling
     * - Type coercion for numbers
     * 
     * @param actual the actual value from the event
     * @param expected the expected value from the rule
     * @return true if values match, false otherwise
     */
    private boolean valuesMatch(Object actual, Object expected) {
        if (actual == null && expected == null) {
            return true;
        }
        if (actual == null || expected == null) {
            return false;
        }
        
        // Handle numeric comparisons
        if (actual instanceof Number && expected instanceof Number) {
            return ((Number) actual).doubleValue() == ((Number) expected).doubleValue();
        }
        
        // Handle string comparisons (case-insensitive)
        String actualStr = actual.toString().toLowerCase().trim();
        String expectedStr = expected.toString().toLowerCase().trim();
        
        return actualStr.equals(expectedStr);
    }
    
    /**
     * Parses the timeframe from a Sigma rule.
     * 
     * Timeframe can be specified as:
     * - A string like "5m", "1h", "30s"
     * - An integer (assumed to be minutes)
     * 
     * @param timeframeObj the timeframe object from the rule
     * @return the timeframe in minutes
     */
    private long parseTimeframe(Object timeframeObj) {
        if (timeframeObj == null) {
            return 5; // Default to 5 minutes
        }
        
        if (timeframeObj instanceof Number) {
            return ((Number) timeframeObj).longValue();
        }
        
        String timeframe = timeframeObj.toString();
        
        // Parse timeframe string (e.g., "5m", "1h", "30s")
        if (timeframe.endsWith("m")) {
            return Long.parseLong(timeframe.substring(0, timeframe.length() - 1));
        } else if (timeframe.endsWith("h")) {
            return Long.parseLong(timeframe.substring(0, timeframe.length() - 1)) * 60;
        } else if (timeframe.endsWith("s")) {
            return Long.parseLong(timeframe.substring(0, timeframe.length() - 1)) / 60;
        }
        
        // Default to parsing as minutes
        try {
            return Long.parseLong(timeframe);
        } catch (NumberFormatException e) {
            log.warn("Failed to parse timeframe: {}, using default 5 minutes", timeframe);
            return 5;
        }
    }
    
    /**
     * Parses the threshold value from a Sigma rule condition.
     * 
     * Condition examples:
     * - "selection | count(source_ip) by user > 5 in 5m"
     * - "selection | count() > 10"
     * 
     * @param condition the condition string from the rule
     * @return the threshold value
     */
    private int parseThreshold(String condition) {
        if (condition == null) {
            return 5; // Default threshold
        }
        
        // Look for "> N" pattern
        int gtIndex = condition.indexOf('>');
        if (gtIndex != -1) {
            String afterGt = condition.substring(gtIndex + 1).trim();
            // Extract the number (may be followed by " in 5m")
            String[] parts = afterGt.split("\\s+");
            if (parts.length > 0) {
                try {
                    return Integer.parseInt(parts[0]);
                } catch (NumberFormatException e) {
                    log.warn("Failed to parse threshold from condition: {}", condition);
                }
            }
        }
        
        return 5; // Default threshold
    }
    
    /**
     * Parses the grouping field from a Sigma rule condition.
     * 
     * Condition examples:
     * - "selection | count(source_ip) by user > 5 in 5m" -> "user"
     * - "selection | count() by source_ip > 10" -> "source_ip"
     * 
     * @param condition the condition string from the rule
     * @return the grouping field name, or "tenant_id" as default
     */
    private String parseGroupByField(String condition) {
        if (condition == null) {
            return "tenant_id";
        }
        
        // Look for "by FIELD" pattern
        int byIndex = condition.indexOf(" by ");
        if (byIndex != -1) {
            String afterBy = condition.substring(byIndex + 4).trim();
            // Extract the field name (may be followed by " > 5")
            String[] parts = afterBy.split("\\s+");
            if (parts.length > 0) {
                return parts[0];
            }
        }
        
        return "tenant_id"; // Default grouping field
    }
    
    /**
     * Creates an Alert from a Sigma rule match.
     * 
     * @param rule the Sigma rule that matched
     * @param counts the aggregated counts that triggered the alert
     * @return a new Alert object
     */
    private Alert createAlert(SigmaRule rule, Map<String, Long> counts) {
        Alert alert = new Alert();
        alert.setId(UUID.randomUUID().toString());
        alert.setTime(Instant.now());
        alert.setRuleId(rule.getId());
        alert.setTitle(rule.getTitle());
        alert.setDescription(rule.getDescription() != null ? rule.getDescription() : "");
        alert.setStatus(AlertStatus.OPEN);
        
        // Map Sigma level to OCSF severity
        alert.setSeverity(mapLevelToSeverity(rule.getLevel()));
        
        // Set tenant ID from the first entry in counts (simplified)
        // In a real implementation, this would be extracted from the keyed stream
        alert.setTenantId("default");
        
        return alert;
    }
    
    /**
     * Maps Sigma rule level to OCSF severity scale.
     * 
     * Sigma levels: informational, low, medium, high, critical
     * OCSF severity: 1=Info, 2=Low, 3=Medium, 4=High, 5=Critical
     * 
     * @param level the Sigma rule level
     * @return the OCSF severity value (1-5)
     */
    private int mapLevelToSeverity(String level) {
        if (level == null) {
            return 3; // Default to Medium
        }
        
        return switch (level.toLowerCase()) {
            case "informational", "info" -> 1;
            case "low" -> 2;
            case "medium" -> 3;
            case "high" -> 4;
            case "critical" -> 5;
            default -> 3; // Default to Medium
        };
    }
    
    /**
     * Aggregate function that counts events by source IP address.
     * 
     * This is used in windowed aggregations to count how many events
     * occurred from each source IP within a time window.
     */
    public static class CountBySourceIp 
        implements AggregateFunction<OcsfEvent, Map<String, Long>, Map<String, Long>> {
        
        private static final long serialVersionUID = 1L;
        
        @Override
        public Map<String, Long> createAccumulator() {
            return new HashMap<>();
        }
        
        @Override
        public Map<String, Long> add(OcsfEvent event, Map<String, Long> accumulator) {
            // For this simplified implementation, we'll use tenant_id as the key
            // In a full implementation, this would extract the actual source IP
            String key = event.getTenantId() != null ? event.getTenantId() : "unknown";
            accumulator.merge(key, 1L, Long::sum);
            return accumulator;
        }
        
        @Override
        public Map<String, Long> getResult(Map<String, Long> accumulator) {
            return accumulator;
        }
        
        @Override
        public Map<String, Long> merge(Map<String, Long> acc1, Map<String, Long> acc2) {
            acc2.forEach((key, value) -> acc1.merge(key, value, Long::sum));
            return acc1;
        }
    }
}
