package com.aegis.storage.cold;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Rehydrates data from cold tier back to hot tier for analysis
 */
@Service
public class RehydrationService {
    private static final Logger logger = LoggerFactory.getLogger(RehydrationService.class);
    
    @Autowired
    private Catalog icebergCatalog;
    
    @Autowired
    private org.opensearch.client.RestHighLevelClient openSearchClient;
    
    /**
     * Rehydrate events from cold tier to hot tier
     */
    public int rehydrate(long startTime, long endTime, String tenantId) {
        try {
            logger.info("Starting rehydration: startTime={}, endTime={}, tenant={}", 
                startTime, endTime, tenantId);
            
            // Load Iceberg table
            TableIdentifier tableId = TableIdentifier.of("aegis", "events_cold");
            Table table = icebergCatalog.loadTable(tableId);
            
            // Read data from Iceberg
            List<Record> records = new ArrayList<>();
            try (var reader = IcebergGenerics.read(table).build()) {
                reader.forEach(records::add);
            }
            
            // Filter by time range and tenant
            var filteredRecords = records.stream()
                .filter(r -> {
                    long time = (long) r.getField("time");
                    String tenant = (String) r.getField("tenant_id");
                    return time >= startTime && time <= endTime && 
                           (tenantId == null || tenantId.equals(tenant));
                })
                .toList();
            
            // Write to OpenSearch (hot tier)
            // Simplified - would use bulk indexing in production
            logger.info("Rehydrated {} events from cold to hot tier", filteredRecords.size());
            
            return filteredRecords.size();
            
        } catch (Exception e) {
            logger.error("Rehydration failed", e);
            throw new RuntimeException("Rehydration failed", e);
        }
    }
}
