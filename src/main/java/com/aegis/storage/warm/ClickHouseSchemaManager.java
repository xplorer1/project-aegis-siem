package com.aegis.storage.warm;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

/**
 * Manages ClickHouse schema creation and updates
 */
@Component
public class ClickHouseSchemaManager {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSchemaManager.class);
    
    @Autowired
    @Qualifier("clickHouseJdbcTemplate")
    private JdbcTemplate jdbcTemplate;
    
    /**
     * Create tables on startup
     */
    @PostConstruct
    public void createTables() {
        try {
            createEventsWarmTable();
            logger.info("ClickHouse schema initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize ClickHouse schema", e);
            // Don't throw - allow application to start
        }
    }
    
    /**
     * Create aegis_events_warm table
     */
    private void createEventsWarmTable() {
        String sql = """
            CREATE TABLE IF NOT EXISTS aegis_events_warm (
                time DateTime64(3),
                category_name LowCardinality(String),
                class_name LowCardinality(String),
                severity UInt8,
                message String,
                actor_user_uid String,
                actor_user_name String,
                src_endpoint_ip IPv4,
                src_endpoint_port UInt16,
                src_endpoint_hostname String,
                dst_endpoint_ip IPv4,
                dst_endpoint_port UInt16,
                dst_endpoint_hostname String,
                metadata String,
                threat_reputation_score Float32,
                threat_level LowCardinality(String),
                ueba_score Float32,
                tenant_id LowCardinality(String),
                event_date Date MATERIALIZED toDate(time)
            )
            ENGINE = MergeTree()
            ORDER BY (tenant_id, event_date, time)
            SETTINGS index_granularity = 8192
            """;
        
        jdbcTemplate.execute(sql);
        logger.info("Created table: aegis_events_warm");
    }
}
