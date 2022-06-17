package com.aegis.storage.warm;

import com.aegis.domain.OcsfEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Migrates events from hot tier (OpenSearch) to warm tier (ClickHouse)
 * Runs daily to move older data to more cost-effective storage
 */
@Component
public class WarmTierMigrator {
    private static final Logger logger = LoggerFactory.getLogger(WarmTierMigrator.class);
    
    @Value("${aegis.storage.migration.hot-to-warm-days:7}")
    private int migrationAgeDays;
    
    @Autowired
    private org.opensearch.client.RestHighLevelClient openSearchClient;
    
    @Autowired
    @org.springframework.beans.factory.annotation.Qualifier("clickHouseJdbcTemplate")
    private org.springframework.jdbc.core.JdbcTemplate clickHouseJdbcTemplate;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    /**
     * Run daily migration at 3 AM
     */
    @Scheduled(cron = "0 0 3 * * *")
    public void scheduledMigration() {
        try {
            logger.info("Starting scheduled warm tier migration");
            long startTime = System.currentTimeMillis();
            
            migrateToWarm();
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Warm tier migration completed in {} ms", duration);
            
        } catch (Exception e) {
            logger.error("Scheduled warm tier migration failed", e);
        }
    }
    
    /**
     * Migrate events from hot to warm tier
     * Exports events from OpenSearch and imports to ClickHouse
     */
    private void migrateToWarm() {
        try {
            logger.info("Migrating events older than {} days to warm tier", migrationAgeDays);
            
            // Calculate cutoff time
            Instant cutoffTime = Instant.now().minus(migrationAgeDays, ChronoUnit.DAYS);
            long cutoffMillis = cutoffTime.toEpochMilli();
            
            // Build query for old events
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.rangeQuery("time").lt(cutoffMillis))
                .size(1000)  // Batch size
                .sort("time", org.opensearch.search.sort.SortOrder.ASC);
            
            SearchRequest searchRequest = new SearchRequest("aegis-events-*")
                .source(sourceBuilder);
            
            // Execute search
            SearchResponse response = openSearchClient.search(searchRequest, RequestOptions.DEFAULT);
            
            if (response.getHits().getTotalHits().value == 0) {
                logger.info("No events to migrate");
                return;
            }
            
            // Parse events
            List<OcsfEvent> events = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                OcsfEvent event = objectMapper.readValue(
                    hit.getSourceAsString(),
                    OcsfEvent.class
                );
                events.add(event);
            }
            
            // Insert into ClickHouse
            insertToClickHouse(events);
            
            logger.info("Migrated {} events to warm tier", events.size());
            
        } catch (Exception e) {
            logger.error("Failed to migrate to warm tier", e);
            throw new RuntimeException("Warm tier migration failed", e);
        }
    }
    
    /**
     * Insert events into ClickHouse
     * Placeholder - will be optimized with batching in next task
     */
    private void insertToClickHouse(List<OcsfEvent> events) {
        String sql = """
            INSERT INTO aegis_events_warm (
                time, category_name, class_name, severity, message,
                actor_user_uid, actor_user_name,
                src_endpoint_ip, src_endpoint_port, src_endpoint_hostname,
                dst_endpoint_ip, dst_endpoint_port, dst_endpoint_hostname,
                metadata, threat_reputation_score, threat_level,
                ueba_score, tenant_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        
        for (OcsfEvent event : events) {
            try {
                clickHouseJdbcTemplate.update(sql,
                    new java.sql.Timestamp(event.getTime()),
                    event.getCategoryName(),
                    event.getClassName(),
                    event.getSeverity(),
                    event.getMessage(),
                    event.getActor() != null && event.getActor().getUser() != null 
                        ? event.getActor().getUser().getUid() : null,
                    event.getActor() != null && event.getActor().getUser() != null 
                        ? event.getActor().getUser().getName() : null,
                    event.getSrcEndpoint() != null ? event.getSrcEndpoint().getIp() : null,
                    event.getSrcEndpoint() != null ? event.getSrcEndpoint().getPort() : null,
                    event.getSrcEndpoint() != null ? event.getSrcEndpoint().getHostname() : null,
                    event.getDstEndpoint() != null ? event.getDstEndpoint().getIp() : null,
                    event.getDstEndpoint() != null ? event.getDstEndpoint().getPort() : null,
                    event.getDstEndpoint() != null ? event.getDstEndpoint().getHostname() : null,
                    event.getMetadata() != null ? objectMapper.writeValueAsString(event.getMetadata()) : null,
                    event.getThreatInfo() != null ? event.getThreatInfo().getReputationScore() : null,
                    event.getThreatInfo() != null ? event.getThreatInfo().getThreatLevel() : null,
                    event.getMetadata() != null ? event.getMetadata().get("ueba_score") : null,
                    event.getMetadata() != null ? event.getMetadata().get("tenant_id") : "default"
                );
            } catch (Exception e) {
                logger.error("Failed to insert event to ClickHouse", e);
            }
        }
    }
}
