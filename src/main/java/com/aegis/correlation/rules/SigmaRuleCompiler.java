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
     * Window types:
     * - Tumbling: Fixed-size, non-overlapping windows (default)
     * - Sliding: Fixed-size, overlapping windows with configurable slide interval
     * - Session: Dynamic windows based on inactivity gaps
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
        
        // Determine window type from detection metadata or default to tumbling
        String windowType = determineWindowType(detection);
        
        // Step 1: Filter events based on selection criteria
        DataStream<OcsfEvent> filtered = events.filter(event -> matchesSelection(event, selection));
        
        // Step 2: Key by grouping field (e.g., user, source IP)
        // For simplicity, we'll use tenantId as the key
        // In a full implementation, this would be dynamic based on groupByField
        DataStream<OcsfEvent> keyed = filtered.keyBy(OcsfEvent::getTenantId);
        
        // Step 3: Apply windowing based on window type and timeframe
        DataStream<Map<String, Long>> aggregated = applyWindowing(
            keyed, windowType, timeframeMinutes, detection
        );
        
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
     * Determines the window type to use for the rule.
     * 
     * Window types can be specified in the detection section using a "window_type" field,
     * or inferred from the condition string. Defaults to "tumbling" if not specified.
     * 
     * Supported window types:
     * - tumbling: Fixed-size, non-overlapping windows
     * - sliding: Fixed-size, overlapping windows
     * - session: Dynamic windows based on inactivity gaps
     * 
     * @param detection the detection section of the Sigma rule
     * @return the window type ("tumbling", "sliding", or "session")
     */
    private String determineWindowType(Map<String, Object> detection) {
        // Check for explicit window_type field
        if (detection.containsKey("window_type")) {
            String windowType = detection.get("window_type").toString().toLowerCase();
            log.debug("Using explicit window type: {}", windowType);
            return windowType;
        }
        
        // Check condition for window type hints
        String condition = (String) detection.get("condition");
        if (condition != null) {
            String lowerCondition = condition.toLowerCase();
            if (lowerCondition.contains("sliding")) {
                log.debug("Inferred sliding window from condition");
                return "sliding";
            } else if (lowerCondition.contains("session")) {
                log.debug("Inferred session window from condition");
                return "session";
            }
        }
        
        // Default to tumbling windows
        log.debug("Using default tumbling window");
        return "tumbling";
    }
    
    /**
     * Applies windowing to the keyed stream based on the specified window type.
     * 
     * This method configures the appropriate Flink window assigner and applies
     * the CountBySourceIp aggregation function.
     * 
     * Tumbling windows:
     * - Fixed-size, non-overlapping time windows
     * - Each event belongs to exactly one window
     * - Example: 5-minute windows [0-5), [5-10), [10-15), ...
     * 
     * @param keyed the keyed stream of events
     * @param windowType the type of window ("tumbling", "sliding", or "session")
     * @param timeframeMinutes the window size in minutes
     * @param detection the detection section for additional window parameters
     * @return a DataStream of aggregated counts
     */
    private DataStream<Map<String, Long>> applyWindowing(
            DataStream<OcsfEvent> keyed,
            String windowType,
            long timeframeMinutes,
            Map<String, Object> detection) {
        
        log.debug("Applying {} window with timeframe: {} minutes", windowType, timeframeMinutes);
        
        return switch (windowType.toLowerCase()) {
            case "tumbling" -> applyTumblingWindow(keyed, timeframeMinutes);
            case "sliding" -> applySlidingWindow(keyed, timeframeMinutes, detection);
            case "session" -> applySessionWindow(keyed, timeframeMinutes, detection);
            default -> {
                log.warn("Unknown window type: {}, defaulting to tumbling", windowType);
                yield applyTumblingWindow(keyed, timeframeMinutes);
            }
        };
    }
    
    /**
     * Applies a tumbling window to the keyed stream.
     * 
     * Tumbling windows are fixed-size, non-overlapping time windows.
     * Each event belongs to exactly one window based on its event time.
     * 
     * Configuration:
     * - Window size: specified by timeframeMinutes
     * - Window alignment: aligned to epoch (e.g., 0:00, 0:05, 0:10 for 5-minute windows)
     * - Trigger: fires when watermark passes window end time
     * 
     * Example with 5-minute windows:
     * - Window 1: [00:00 - 00:05)
     * - Window 2: [00:05 - 00:10)
     * - Window 3: [00:10 - 00:15)
     * 
     * @param keyed the keyed stream of events
     * @param timeframeMinutes the window size in minutes
     * @return a DataStream of aggregated counts
     */
    private DataStream<Map<String, Long>> applyTumblingWindow(
            DataStream<OcsfEvent> keyed,
            long timeframeMinutes) {
        
        log.info("Configuring tumbling window: size = {} minutes", timeframeMinutes);
        
        return keyed
            .window(TumblingEventTimeWindows.of(Time.minutes(timeframeMinutes)))
            .aggregate(new CountBySourceIp());
    }
    
    /**
     * Applies a sliding window to the keyed stream.
     * 
     * Sliding windows are fixed-size, overlapping time windows that slide forward
     * at regular intervals. Each event can belong to multiple windows.
     * 
     * Configuration:
     * - Window size: specified by timeframeMinutes
     * - Slide interval: specified in detection section or defaults to half the window size
     * - Window alignment: aligned to epoch
     * - Trigger: fires when watermark passes window end time
     * 
     * Example with 10-minute windows sliding every 5 minutes:
     * - Window 1: [00:00 - 00:10)
     * - Window 2: [00:05 - 00:15)
     * - Window 3: [00:10 - 00:20)
     * 
     * An event at 00:07 would belong to both Window 1 and Window 2.
     * 
     * Use cases:
     * - Detecting patterns that span window boundaries
     * - More frequent updates (every slide interval vs every window size)
     * - Smoothing out detection over time
     * 
     * @param keyed the keyed stream of events
     * @param timeframeMinutes the window size in minutes
     * @param detection the detection section for slide interval configuration
     * @return a DataStream of aggregated counts
     */
    private DataStream<Map<String, Long>> applySlidingWindow(
            DataStream<OcsfEvent> keyed,
            long timeframeMinutes,
            Map<String, Object> detection) {
        
        // Parse slide interval from detection section
        // Default to half the window size if not specified
        long slideMinutes = parseSlideInterval(detection, timeframeMinutes);
        
        log.info("Configuring sliding window: size = {} minutes, slide = {} minutes", 
            timeframeMinutes, slideMinutes);
        
        return keyed
            .window(SlidingEventTimeWindows.of(
                Time.minutes(timeframeMinutes),
                Time.minutes(slideMinutes)
            ))
            .aggregate(new CountBySourceIp());
    }
    
    /**
     * Parses the slide interval for sliding windows from the detection section.
     * 
     * The slide interval can be specified using:
     * - "slide_interval" field (e.g., "2m", "30s", "1h")
     * - "slide" field (alternative name)
     * 
     * If not specified, defaults to half the window size for 50% overlap.
     * 
     * @param detection the detection section of the Sigma rule
     * @param windowSizeMinutes the window size in minutes
     * @return the slide interval in minutes
     */
    private long parseSlideInterval(Map<String, Object> detection, long windowSizeMinutes) {
        // Check for explicit slide_interval field
        if (detection.containsKey("slide_interval")) {
            return parseTimeframe(detection.get("slide_interval"));
        }
        
        // Check for alternative "slide" field
        if (detection.containsKey("slide")) {
            return parseTimeframe(detection.get("slide"));
        }
        
        // Default to half the window size for 50% overlap
        long defaultSlide = windowSizeMinutes / 2;
        log.debug("No slide interval specified, using default: {} minutes (50% of window size)", 
            defaultSlide);
        return defaultSlide;
    }
    
    /**
     * Applies a session window to the keyed stream.
     * 
     * Session windows are dynamic windows that group events based on periods of activity
     * separated by gaps of inactivity. Unlike tumbling and sliding windows, session windows
     * have variable length and are not aligned to a fixed time grid.
     * 
     * Configuration:
     * - Gap duration: specified in detection section or uses timeframeMinutes as default
     * - Window creation: new window starts when gap exceeds threshold
     * - Window closure: window closes when no events arrive for gap duration
     * - Merging: overlapping sessions are automatically merged
     * 
     * Example with 5-minute gap:
     * - Events at 00:00, 00:02, 00:04 -> Session 1: [00:00 - 00:09)
     * - Gap from 00:04 to 00:12 (8 minutes > 5 minute gap)
     * - Events at 00:12, 00:14 -> Session 2: [00:12 - 00:19)
     * 
     * Use cases:
     * - User session analysis (login to logout)
     * - Attack campaign detection (bursts of activity)
     * - Application transaction tracking
     * - Network connection analysis
     * 
     * @param keyed the keyed stream of events
     * @param timeframeMinutes the default gap duration in minutes
     * @param detection the detection section for gap configuration
     * @return a DataStream of aggregated counts
     */
    private DataStream<Map<String, Long>> applySessionWindow(
            DataStream<OcsfEvent> keyed,
            long timeframeMinutes,
            Map<String, Object> detection) {
        
        // Parse session gap from detection section
        // Default to timeframeMinutes if not specified
        long gapMinutes = parseSessionGap(detection, timeframeMinutes);
        
        log.info("Configuring session window: gap = {} minutes", gapMinutes);
        
        return keyed
            .window(EventTimeSessionWindows.withGap(Time.minutes(gapMinutes)))
            .aggregate(new CountBySourceIp());
    }
    
    /**
     * Parses the session gap duration from the detection section.
     * 
     * The session gap can be specified using:
     * - "session_gap" field (e.g., "5m", "30s", "1h")
     * - "gap" field (alternative name)
     * - "inactivity_gap" field (descriptive name)
     * 
     * The gap duration determines how long to wait for new events before
     * closing a session window. If events arrive within the gap, they are
     * added to the current session. If the gap is exceeded, a new session starts.
     * 
     * If not specified, defaults to the timeframe value.
     * 
     * @param detection the detection section of the Sigma rule
     * @param defaultGapMinutes the default gap duration in minutes
     * @return the session gap in minutes
     */
    private long parseSessionGap(Map<String, Object> detection, long defaultGapMinutes) {
        // Check for explicit session_gap field
        if (detection.containsKey("session_gap")) {
            return parseTimeframe(detection.get("session_gap"));
        }
        
        // Check for alternative "gap" field
        if (detection.containsKey("gap")) {
            return parseTimeframe(detection.get("gap"));
        }
        
        // Check for descriptive "inactivity_gap" field
        if (detection.containsKey("inactivity_gap")) {
            return parseTimeframe(detection.get("inactivity_gap"));
        }
        
        // Default to the timeframe value
        log.debug("No session gap specified, using default: {} minutes", defaultGapMinutes);
        return defaultGapMinutes;
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
     * occurred from each source IP within a time window. The aggregator
     * maintains a map where keys are source IP addresses and values are
     * the count of events from that IP.
     * 
     * This is particularly useful for detecting:
     * - Brute force attacks (many failed logins from same IP)
     * - Port scans (many connection attempts from same IP)
     * - DDoS attacks (high volume from specific IPs)
     * 
     * The aggregator is designed to work with Flink's windowing operators
     * and supports both tumbling and sliding windows.
     */
    public static class CountBySourceIp 
        implements AggregateFunction<OcsfEvent, Map<String, Long>, Map<String, Long>> {
        
        private static final long serialVersionUID = 1L;
        
        /**
         * Creates a new empty accumulator for counting events.
         * 
         * @return an empty HashMap to store IP -> count mappings
         */
        @Override
        public Map<String, Long> createAccumulator() {
            return new HashMap<>();
        }
        
        /**
         * Adds an event to the accumulator, incrementing the count for its source IP.
         * 
         * Extracts the source IP from the event's source endpoint. If no source IP
         * is available, uses "unknown" as the key. This ensures events without
         * source IPs are still counted and can trigger alerts if needed.
         * 
         * @param event the OCSF event to add
         * @param accumulator the current accumulator state
         * @return the updated accumulator
         */
        @Override
        public Map<String, Long> add(OcsfEvent event, Map<String, Long> accumulator) {
            // Extract source IP from the event
            String sourceIp = "unknown";
            
            if (event.getSrcEndpoint() != null && event.getSrcEndpoint().getIp() != null) {
                sourceIp = event.getSrcEndpoint().getIp();
            }
            
            // Increment the count for this source IP
            accumulator.merge(sourceIp, 1L, Long::sum);
            
            return accumulator;
        }
        
        /**
         * Returns the final result from the accumulator.
         * 
         * @param accumulator the accumulator containing IP -> count mappings
         * @return the final map of source IPs to event counts
         */
        @Override
        public Map<String, Long> getResult(Map<String, Long> accumulator) {
            return accumulator;
        }
        
        /**
         * Merges two accumulators together.
         * 
         * This is used when Flink needs to combine partial aggregates from
         * different parallel instances or when merging session windows.
         * 
         * @param acc1 the first accumulator
         * @param acc2 the second accumulator
         * @return the merged accumulator containing combined counts
         */
        @Override
        public Map<String, Long> merge(Map<String, Long> acc1, Map<String, Long> acc2) {
            acc2.forEach((key, value) -> acc1.merge(key, value, Long::sum));
            return acc1;
        }
    }
}
