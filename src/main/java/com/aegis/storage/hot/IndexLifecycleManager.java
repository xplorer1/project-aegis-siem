package com.aegis.storage.hot;

import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.rollover.RolloverRequest;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

/**
 * Manages index lifecycle policies for hot tier storage
 * Handles index rollover and retention
 */
@Component
public class IndexLifecycleManager {
    private static final Logger logger = LoggerFactory.getLogger(IndexLifecycleManager.class);
    
    @Value("${aegis.storage.hot.retention-days:7}")
    private int retentionDays;
    
    @Value("${aegis.storage.hot.rollover-size-gb:50}")
    private int rolloverSizeGb;
    
    @Value("${aegis.storage.hot.rollover-age-hours:24}")
    private int rolloverAgeHours;
    
    @Autowired
    private RestHighLevelClient client;
    
    @PostConstruct
    public void init() {
        logger.info("Index lifecycle policy configured:");
        logger.info("  Hot tier retention: {} days", retentionDays);
        logger.info("  Rollover size: {} GB", rolloverSizeGb);
        logger.info("  Rollover age: {} hours", rolloverAgeHours);
    }
    
    /**
     * Check and perform index rollover if conditions are met
     * Runs every hour
     */
    @Scheduled(cron = "0 0 * * * *")
    public void checkRollover() {
        try {
            String alias = "aegis-events";
            
            RolloverRequest request = new RolloverRequest(alias, null);
            
            // Set rollover conditions
            request.addMaxIndexSizeCondition(new ByteSizeValue(rolloverSizeGb, ByteSizeUnit.GB));
            request.addMaxIndexAgeCondition(new TimeValue(rolloverAgeHours, TimeUnit.HOURS));
            request.addMaxIndexDocsCondition(100_000_000L); // 100M documents
            
            // Execute rollover
            var response = client.indices().rollover(request, RequestOptions.DEFAULT);
            
            if (response.isRolledOver()) {
                logger.info("Index rolled over: old={}, new={}", 
                    response.getOldIndex(), response.getNewIndex());
            } else {
                logger.debug("Rollover conditions not met");
            }
            
        } catch (Exception e) {
            logger.error("Failed to check/perform rollover", e);
        }
    }
    
    /**
     * Delete old indices beyond retention period
     * Runs daily at 2 AM
     */
    @Scheduled(cron = "0 0 2 * * *")
    public void cleanupOldIndices() {
        try {
            long cutoffTime = System.currentTimeMillis() - 
                (retentionDays * 24L * 60 * 60 * 1000);
            
            // Get all aegis-events-* indices
            var response = client.indices().get(
                new org.opensearch.client.indices.GetIndexRequest("aegis-events-*"),
                RequestOptions.DEFAULT
            );
            
            for (String indexName : response.getIndices()) {
                // Extract date from index name (aegis-events-2022-06-09)
                if (shouldDeleteIndex(indexName, cutoffTime)) {
                    client.indices().delete(
                        new org.opensearch.client.indices.DeleteIndexRequest(indexName),
                        RequestOptions.DEFAULT
                    );
                    logger.info("Deleted old index: {}", indexName);
                }
            }
            
        } catch (Exception e) {
            logger.error("Failed to cleanup old indices", e);
        }
    }
    
    private boolean shouldDeleteIndex(String indexName, long cutoffTime) {
        try {
            // Extract date from index name (aegis-events-2022-06-09)
            String[] parts = indexName.split("-");
            if (parts.length >= 5) {
                int year = Integer.parseInt(parts[2]);
                int month = Integer.parseInt(parts[3]);
                int day = Integer.parseInt(parts[4]);
                
                java.time.LocalDate indexDate = java.time.LocalDate.of(year, month, day);
                long indexTime = indexDate.atStartOfDay(java.time.ZoneOffset.UTC)
                    .toInstant().toEpochMilli();
                
                return indexTime < cutoffTime;
            }
        } catch (Exception e) {
            logger.warn("Failed to parse index date from: {}", indexName);
        }
        return false;
    }
}
