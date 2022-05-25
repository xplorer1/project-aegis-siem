package com.aegis.correlation.rules;

import com.aegis.domain.Alert;
import com.aegis.domain.OcsfEvent;
import com.aegis.domain.SigmaRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for SigmaRuleCompiler.
 * 
 * Tests the compilation of Sigma rules and event matching logic.
 */
class SigmaRuleCompilerTest {
    
    private SigmaRuleCompiler compiler;
    
    @BeforeEach
    void setUp() {
        compiler = new SigmaRuleCompiler();
    }
    
    @Test
    void testMatchesSelection_withMatchingEvent() {
        // Given: An event with specific field values
        OcsfEvent event = new OcsfEvent();
        event.setClassUid(3002); // Authentication event
        event.setSeverity(3);
        event.setTenantId("tenant-123");
        
        // And: Selection criteria that matches the event
        Map<String, Object> selection = new HashMap<>();
        selection.put("class_uid", 3002);
        selection.put("severity", 3);
        
        // When: Checking if event matches selection
        boolean matches = compiler.matchesSelection(event, selection);
        
        // Then: Event should match
        assertThat(matches).isTrue();
    }
    
    @Test
    void testMatchesSelection_withNonMatchingEvent() {
        // Given: An event with specific field values
        OcsfEvent event = new OcsfEvent();
        event.setClassUid(3002);
        event.setSeverity(2);
        
        // And: Selection criteria that doesn't match the event
        Map<String, Object> selection = new HashMap<>();
        selection.put("class_uid", 3002);
        selection.put("severity", 4); // Different severity
        
        // When: Checking if event matches selection
        boolean matches = compiler.matchesSelection(event, selection);
        
        // Then: Event should not match
        assertThat(matches).isFalse();
    }
    
    @Test
    void testMatchesSelection_withEmptySelection() {
        // Given: An event
        OcsfEvent event = new OcsfEvent();
        event.setClassUid(3002);
        
        // And: Empty selection criteria
        Map<String, Object> selection = new HashMap<>();
        
        // When: Checking if event matches selection
        boolean matches = compiler.matchesSelection(event, selection);
        
        // Then: Event should match (empty selection matches all)
        assertThat(matches).isTrue();
    }
    
    @Test
    void testMatchesSelection_withNullSelection() {
        // Given: An event
        OcsfEvent event = new OcsfEvent();
        event.setClassUid(3002);
        
        // When: Checking if event matches null selection
        boolean matches = compiler.matchesSelection(event, null);
        
        // Then: Event should match (null selection matches all)
        assertThat(matches).isTrue();
    }
    
    @Test
    void testCountBySourceIp_aggregation() {
        // Given: An aggregate function
        SigmaRuleCompiler.CountBySourceIp aggregator = new SigmaRuleCompiler.CountBySourceIp();
        
        // And: An accumulator
        Map<String, Long> accumulator = aggregator.createAccumulator();
        
        // When: Adding events with the same tenant
        OcsfEvent event1 = new OcsfEvent();
        event1.setTenantId("tenant-123");
        
        OcsfEvent event2 = new OcsfEvent();
        event2.setTenantId("tenant-123");
        
        OcsfEvent event3 = new OcsfEvent();
        event3.setTenantId("tenant-456");
        
        accumulator = aggregator.add(event1, accumulator);
        accumulator = aggregator.add(event2, accumulator);
        accumulator = aggregator.add(event3, accumulator);
        
        // Then: Accumulator should have correct counts
        Map<String, Long> result = aggregator.getResult(accumulator);
        assertThat(result).containsEntry("tenant-123", 2L);
        assertThat(result).containsEntry("tenant-456", 1L);
    }
    
    @Test
    void testCountBySourceIp_merge() {
        // Given: An aggregate function
        SigmaRuleCompiler.CountBySourceIp aggregator = new SigmaRuleCompiler.CountBySourceIp();
        
        // And: Two accumulators with different counts
        Map<String, Long> acc1 = new HashMap<>();
        acc1.put("tenant-123", 2L);
        acc1.put("tenant-456", 1L);
        
        Map<String, Long> acc2 = new HashMap<>();
        acc2.put("tenant-123", 3L);
        acc2.put("tenant-789", 1L);
        
        // When: Merging accumulators
        Map<String, Long> merged = aggregator.merge(acc1, acc2);
        
        // Then: Merged accumulator should have summed counts
        assertThat(merged).containsEntry("tenant-123", 5L);
        assertThat(merged).containsEntry("tenant-456", 1L);
        assertThat(merged).containsEntry("tenant-789", 1L);
    }
    
    @Test
    void testSigmaRuleYamlParsing() throws Exception {
        // Given: A Sigma rule in YAML format
        String yaml = """
            id: test-rule-001
            title: Test Brute Force Detection
            status: stable
            description: Detects multiple failed login attempts
            logsource:
              category: authentication
            detection:
              selection:
                event_type: authentication
                outcome: failure
              condition: selection | count(source_ip) by user > 5 in 5m
              timeframe: 5m
            level: high
            """;
        
        // When: Parsing the YAML
        SigmaRule rule = SigmaRule.fromYaml(yaml);
        
        // Then: Rule should be parsed correctly
        assertThat(rule).isNotNull();
        assertThat(rule.getId()).isEqualTo("test-rule-001");
        assertThat(rule.getTitle()).isEqualTo("Test Brute Force Detection");
        assertThat(rule.getStatus()).isEqualTo("stable");
        assertThat(rule.getLevel()).isEqualTo("high");
        assertThat(rule.getDetection()).isNotNull();
        assertThat(rule.getDetection()).containsKey("selection");
        assertThat(rule.getDetection()).containsKey("condition");
    }
}
