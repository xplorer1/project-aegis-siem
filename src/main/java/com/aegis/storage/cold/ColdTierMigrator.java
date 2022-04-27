package com.aegis.storage.cold;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Migrates events from warm tier (ClickHouse) to cold tier (Iceberg)
 * Runs daily to move older data to long-term archival storage
 */
@Component
public class ColdTierMigrator {
    private static final Logger logger = LoggerFactory.getLogger(ColdTierMigrator.class);
    
    @Value("${aegis.storage.migration.warm-to-cold-days:90}")
    private int migrationAgeDays;
    
    @Autowired
    @Qualifier("clickHouseJdbcTemplate")
    private JdbcTemplate clickHouseJdbcTemplate;
    
    @Autowired
    private org.apache.iceberg.catalog.Catalog icebergCatalog;
    
    @Autowired
    private ParquetWriter parquetWriter;
    
    /**
     * Run daily migration at 4 AM
     */
    @Scheduled(cron = "0 0 4 * * *")
    public void scheduledMigration() {
        try {
            logger.info("Starting scheduled cold tier migration");
            long startTime = System.currentTimeMillis();
            
            migrateToCold();
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Cold tier migration completed in {} ms", duration);
            
        } catch (Exception e) {
            logger.error("Scheduled cold tier migration failed", e);
        }
    }
    
    /**
     * Migrate events from warm to cold tier
     * Exports from ClickHouse and writes to Parquet/Iceberg
     */
    private void migrateToCold() {
        try {
            logger.info("Migrating events older than {} days to cold tier", migrationAgeDays);
            
            // Calculate cutoff time
            long cutoffMillis = System.currentTimeMillis() - 
                (migrationAgeDays * 24L * 60 * 60 * 1000);
            
            // Query old events from ClickHouse
            String sql = """
                SELECT 
                    time, tenant_id, category_name, class_name, severity, message,
                    actor_user_uid, actor_user_name,
                    src_endpoint_ip, src_endpoint_port, src_endpoint_hostname,
                    dst_endpoint_ip, dst_endpoint_port, dst_endpoint_hostname,
                    metadata, threat_reputation_score, threat_level, ueba_score
                FROM aegis_events_warm
                WHERE toUnixTimestamp64Milli(time) < ?
                LIMIT 100000
                """;
            
            var events = clickHouseJdbcTemplate.queryForList(sql, cutoffMillis);
            
            if (events.isEmpty()) {
                logger.info("No events to migrate to cold tier");
                return;
            }
            
            // Write to Parquet
            String parquetPath = String.format("/tmp/aegis-cold-%d.parquet", 
                System.currentTimeMillis());
            parquetWriter.writeToParquet(events, parquetPath);
            
            // Register with Iceberg
            registerWithIceberg(parquetPath, events.size());
            
            logger.info("Migrated {} events to cold tier", events.size());
            
        } catch (Exception e) {
            logger.error("Failed to migrate to cold tier", e);
            throw new RuntimeException("Cold tier migration failed", e);
        }
    }
    
    /**
     * Register Parquet file with Iceberg table
     */
    private void registerWithIceberg(String parquetPath, long recordCount) {
        try {
            TableIdentifier tableId = TableIdentifier.of("aegis", "events_cold");
            Table table = icebergCatalog.loadTable(tableId);
            
            // Create DataFile metadata
            DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(parquetPath)
                .withFileSizeInBytes(new java.io.File(parquetPath).length())
                .withRecordCount(recordCount)
                .build();
            
            // Append to table
            table.newAppend()
                .appendFile(dataFile)
                .commit();
            
            logger.info("Registered Parquet file with Iceberg: {}", parquetPath);
            
        } catch (Exception e) {
            logger.error("Failed to register file with Iceberg", e);
            throw new RuntimeException("Iceberg registration failed", e);
        }
    }
}
