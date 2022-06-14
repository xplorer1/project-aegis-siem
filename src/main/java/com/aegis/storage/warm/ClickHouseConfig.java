package com.aegis.storage.warm;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * Configuration for ClickHouse JDBC connection
 * Manages connection pool for warm tier storage
 */
@Configuration
public class ClickHouseConfig {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseConfig.class);
    
    @Value("${aegis.storage.clickhouse.url:jdbc:clickhouse://localhost:8123/aegis}")
    private String url;
    
    @Value("${aegis.storage.clickhouse.username:default}")
    private String username;
    
    @Value("${aegis.storage.clickhouse.password:}")
    private String password;
    
    @Value("${aegis.storage.clickhouse.pool.size:10}")
    private int poolSize;
    
    /**
     * Create ClickHouse DataSource with connection pooling
     */
    @Bean(name = "clickHouseDataSource")
    public DataSource clickHouseDataSource() {
        try {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url);
            config.setUsername(username);
            config.setPassword(password);
            config.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
            
            // Connection pool settings
            config.setMaximumPoolSize(poolSize);
            config.setMinimumIdle(2);
            config.setConnectionTimeout(30000);
            config.setIdleTimeout(600000);
            config.setMaxLifetime(1800000);
            
            // ClickHouse-specific settings
            config.addDataSourceProperty("socket_timeout", "300000");
            config.addDataSourceProperty("compress", "true");
            config.addDataSourceProperty("max_execution_time", "300");
            
            HikariDataSource dataSource = new HikariDataSource(config);
            
            logger.info("ClickHouse DataSource initialized: {}", url);
            return dataSource;
            
        } catch (Exception e) {
            logger.error("Failed to initialize ClickHouse DataSource", e);
            throw new RuntimeException("ClickHouse DataSource initialization failed", e);
        }
    }
    
    /**
     * Create JdbcTemplate for ClickHouse operations
     */
    @Bean(name = "clickHouseJdbcTemplate")
    public JdbcTemplate clickHouseJdbcTemplate(DataSource clickHouseDataSource) {
        return new JdbcTemplate(clickHouseDataSource);
    }
}
