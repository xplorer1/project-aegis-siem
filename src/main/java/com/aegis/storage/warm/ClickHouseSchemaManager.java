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
            createHourlyAggregationView();
            logger.info("ClickHouse schema initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize ClickHouse schema", e);
            // Don't throw - allow application to start
        }
    }
    
    /**
     * Create aegis_events_warm table with partitioning and TTL
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
            PARTITION BY toYYYYMM(event_date)
            ORDER BY (tenant_id, event_date, time)
            TTL event_date + INTERVAL 90 DAY
            SETTINGS index_granularity = 8192
            """;
        
        jdbcTemplate.execute(sql);
        logger.info("Created table: aegis_events_warm with monthly partitioning and 90-day TTL");
    }
    
    /**
     * Create materialized view for hourly aggregations
     */
    private void createHourlyAggregationView() {
        // First create the target table for the materialized view
        String targetTableSql = """
            CREATE TABLE IF NOT EXISTS aegis_events_hourly (
                event_hour DateTime,
                tenant_id LowCardinality(String),
                category_name LowCardinality(String),
                severity UInt8,
                event_count UInt64,
                unique_users UInt64,
                unique_src_ips UInt64,
                avg_ueba_score Float32
            )
            ENGINE = SummingMergeTree()
            PARTITION BY toYYYYMM(event_hour)
            ORDER BY (tenant_id, event_hour, category_name, severity)
            TTL event_hour + INTERVAL 180 DAY
            """;
        
        jdbcTemplate.execute(targetTableSql);
        logger.info("Created table: aegis_events_hourly");
        
        // Create materialized view
        String viewSql = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS aegis_events_hourly_mv
            TO aegis_events_hourly
            AS SELECT
                toStartOfHour(time) AS event_hour,
                tenant_id,
                category_name,
                severity,
                count() AS event_count,
                uniq(actor_user_uid) AS unique_users,
                uniq(src_endpoint_ip) AS unique_src_ips,
                avg(ueba_score) AS avg_ueba_score
            FROM aegis_events_warm
            GROUP BY event_hour, tenant_id, category_name, severity
            """;
        
        jdbcTemplate.execute(viewSql);
        logger.info("Created materialized view: aegis_events_hourly_mv");
    }
}
