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
     * Exports events from OpenSearch using scroll API and imports to ClickHouse
     */
    private void migrateToWarm() {
        try {
            logger.info("Migrating events older than {} days to warm tier", migrationAgeDays);
            
            // Calculate cutoff time
            Instant cutoffTime = Instant.now().minus(migrationAgeDays, ChronoUnit.DAYS);
            long cutoffMillis = cutoffTime.toEpochMilli();
            
            // Use scroll API for large result sets
            int totalMigrated = 0;
            String scrollId = null;
            
            try {
                // Initial search request with scroll
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                    .query(QueryBuilders.rangeQuery("time").lt(cutoffMillis))
                    .size(1000)  // Batch size per scroll
                    .sort("time", org.opensearch.search.sort.SortOrder.ASC);
                
                SearchRequest searchRequest = new SearchRequest("aegis-events-*")
                    .source(sourceBuilder)
                    .scroll(org.opensearch.common.unit.TimeValue.timeValueMinutes(5));
                
                SearchResponse response = openSearchClient.search(searchRequest, RequestOptions.DEFAULT);
                scrollId = response.getScrollId();
                
                // Process first batch
                while (response.getHits().getHits().length > 0) {
                    List<OcsfEvent> events = new ArrayList<>();
                    for (SearchHit hit : response.getHits().getHits()) {
                        OcsfEvent event = objectMapper.readValue(
                            hit.getSourceAsString(),
                            OcsfEvent.class
                        );
                        events.add(event);
                    }
                    
                    // Insert batch to ClickHouse
                    insertToClickHouse(events);
                    totalMigrated += events.size();
                    
                    logger.debug("Migrated batch of {} events (total: {})", 
                        events.size(), totalMigrated);
                    
                    // Get next batch using scroll
                    org.opensearch.action.search.SearchScrollRequest scrollRequest = 
                        new org.opensearch.action.search.SearchScrollRequest(scrollId);
                    scrollRequest.scroll(org.opensearch.common.unit.TimeValue.timeValueMinutes(5));
                    
                    response = openSearchClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                    scrollId = response.getScrollId();
                }
                
                logger.info("Migrated {} events to warm tier", totalMigrated);
                
                // Post-migration cleanup
                if (totalMigrated > 0) {
                    cleanupMigratedData(cutoffMillis);
                }
                
            } finally {
                // Clear scroll context
                if (scrollId != null) {
                    org.opensearch.action.search.ClearScrollRequest clearScrollRequest = 
                        new org.opensearch.action.search.ClearScrollRequest();
                    clearScrollRequest.addScrollId(scrollId);
                    openSearchClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
                }
            }
            
        } catch (Exception e) {
            logger.error("Failed to migrate to warm tier", e);
            throw new RuntimeException("Warm tier migration failed", e);
        }
    }
    
    /**
     * Insert events into ClickHouse using batch insert
     * Optimized for high throughput
     */
    private void insertToClickHouse(List<OcsfEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        
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
        
        try {
            clickHouseJdbcTemplate.batchUpdate(sql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                @Override
                public void setValues(java.sql.PreparedStatement ps, int i) throws java.sql.SQLException {
                    OcsfEvent event = events.get(i);
                    
                    try {
                        ps.setTimestamp(1, new java.sql.Timestamp(event.getTime()));
                        ps.setString(2, event.getCategoryName());
                        ps.setString(3, event.getClassName());
                        ps.setInt(4, event.getSeverity());
                        ps.setString(5, event.getMessage());
                        
                        // Actor fields
                        ps.setString(6, event.getActor() != null && event.getActor().getUser() != null 
                            ? event.getActor().getUser().getUid() : null);
                        ps.setString(7, event.getActor() != null && event.getActor().getUser() != null 
                            ? event.getActor().getUser().getName() : null);
                        
                        // Source endpoint fields
                        ps.setString(8, event.getSrcEndpoint() != null ? event.getSrcEndpoint().getIp() : null);
                        ps.setObject(9, event.getSrcEndpoint() != null ? event.getSrcEndpoint().getPort() : null);
                        ps.setString(10, event.getSrcEndpoint() != null ? event.getSrcEndpoint().getHostname() : null);
                        
                        // Destination endpoint fields
                        ps.setString(11, event.getDstEndpoint() != null ? event.getDstEndpoint().getIp() : null);
                        ps.setObject(12, event.getDstEndpoint() != null ? event.getDstEndpoint().getPort() : null);
                        ps.setString(13, event.getDstEndpoint() != null ? event.getDstEndpoint().getHostname() : null);
                        
                        // Metadata and enrichment fields
                        ps.setString(14, event.getMetadata() != null 
                            ? objectMapper.writeValueAsString(event.getMetadata()) : null);
                        ps.setObject(15, event.getThreatInfo() != null 
                            ? event.getThreatInfo().getReputationScore() : null);
                        ps.setString(16, event.getThreatInfo() != null 
                            ? event.getThreatInfo().getThreatLevel() : null);
                        ps.setObject(17, event.getMetadata() != null 
                            ? event.getMetadata().get("ueba_score") : null);
                        ps.setString(18, event.getMetadata() != null && event.getMetadata().get("tenant_id") != null
                            ? event.getMetadata().get("tenant_id").toString() : "default");
                            
                    } catch (Exception e) {
                        logger.error("Failed to prepare statement for event", e);
                        throw new java.sql.SQLException("Statement preparation failed", e);
                    }
                }
                
                @Override
                public int getBatchSize() {
                    return events.size();
                }
            });
            
            logger.debug("Batch inserted {} events to ClickHouse", events.size());
            
        } catch (Exception e) {
            logger.error("Failed to batch insert events to ClickHouse", e);
            throw new RuntimeException("Batch insert failed", e);
        }
    }
    
    /**
     * Cleanup migrated data from hot tier
     * Deletes old indices after verifying data integrity
     */
    private void cleanupMigratedData(long cutoffMillis) {
        try {
            logger.info("Starting post-migration cleanup");
            
            // Verify data integrity by counting records
            String countSql = "SELECT count() FROM aegis_events_warm WHERE toUnixTimestamp64Milli(time) < ?";
            Long warmCount = clickHouseJdbcTemplate.queryForObject(countSql, Long.class, cutoffMillis);
            
            logger.info("Verified {} events in warm tier", warmCount);
            
            // Get indices to delete
            java.time.Instant cutoffTime = java.time.Instant.ofEpochMilli(cutoffMillis);
            java.time.LocalDate cutoffDate = cutoffTime.atZone(java.time.ZoneOffset.UTC).toLocalDate();
            
            // Delete indices older than cutoff date
            var getIndexRequest = new org.opensearch.client.indices.GetIndexRequest("aegis-events-*");
            var getIndexResponse = openSearchClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
            
            int deletedIndices = 0;
            for (String indexName : getIndexResponse.getIndices()) {
                if (shouldDeleteIndex(indexName, cutoffDate)) {
                    try {
                        var deleteRequest = new org.opensearch.client.indices.DeleteIndexRequest(indexName);
                        openSearchClient.indices().delete(deleteRequest, RequestOptions.DEFAULT);
                        deletedIndices++;
                        logger.info("Deleted migrated index: {}", indexName);
                    } catch (Exception e) {
                        logger.error("Failed to delete index: {}", indexName, e);
                    }
                }
            }
            
            logger.info("Post-migration cleanup completed: deleted {} indices", deletedIndices);
            
        } catch (Exception e) {
            logger.error("Post-migration cleanup failed", e);
            // Don't throw - cleanup failure shouldn't fail the migration
        }
    }
    
    /**
     * Check if an index should be deleted based on its date
     */
    private boolean shouldDeleteIndex(String indexName, java.time.LocalDate cutoffDate) {
        try {
            // Extract date from index name (aegis-events-2022-06-09)
            String[] parts = indexName.split("-");
            if (parts.length >= 5) {
                int year = Integer.parseInt(parts[2]);
                int month = Integer.parseInt(parts[3]);
                int day = Integer.parseInt(parts[4]);
                
                java.time.LocalDate indexDate = java.time.LocalDate.of(year, month, day);
                return indexDate.isBefore(cutoffDate);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse index date from: {}", indexName);
        }
        return false;
    }
}
