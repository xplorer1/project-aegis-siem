package com.aegis.storage.warm;

import com.aegis.domain.OcsfEvent;
import com.aegis.domain.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Repository for querying events from ClickHouse warm tier
 */
@Repository
public class ClickHouseRepository {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseRepository.class);
    
    @Autowired
    @Qualifier("clickHouseJdbcTemplate")
    private JdbcTemplate jdbcTemplate;
    
    /**
     * Search events by time range
     */
    public QueryResult<OcsfEvent> searchByTimeRange(long startTime, long endTime, int limit) {
        String sql = """
            SELECT 
                time, category_name, class_name, severity, message,
                actor_user_uid, actor_user_name,
                src_endpoint_ip, src_endpoint_port, src_endpoint_hostname,
                dst_endpoint_ip, dst_endpoint_port, dst_endpoint_hostname,
                metadata, threat_reputation_score, threat_level,
                ueba_score, tenant_id
            FROM aegis_events_warm
            WHERE toUnixTimestamp64Milli(time) >= ? 
              AND toUnixTimestamp64Milli(time) <= ?
            ORDER BY time DESC
            LIMIT ?
            """;
        
        long startQuery = System.currentTimeMillis();
        List<OcsfEvent> events = jdbcTemplate.query(sql, new OcsfEventRowMapper(), 
            startTime, endTime, limit);
        long duration = System.currentTimeMillis() - startQuery;
        
        QueryResult<OcsfEvent> result = new QueryResult<>();
        result.setResults(events);
        result.setTotal(events.size());
        result.setTookMs(duration);
        
        logger.debug("Query returned {} results in {} ms", events.size(), duration);
        return result;
    }
    
    /**
     * Search events by category
     */
    public QueryResult<OcsfEvent> searchByCategory(String category, int limit) {
        String sql = """
            SELECT 
                time, category_name, class_name, severity, message,
                actor_user_uid, actor_user_name,
                src_endpoint_ip, src_endpoint_port, src_endpoint_hostname,
                dst_endpoint_ip, dst_endpoint_port, dst_endpoint_hostname,
                metadata, threat_reputation_score, threat_level,
                ueba_score, tenant_id
            FROM aegis_events_warm
            WHERE category_name = ?
            ORDER BY time DESC
            LIMIT ?
            """;
        
        long startQuery = System.currentTimeMillis();
        List<OcsfEvent> events = jdbcTemplate.query(sql, new OcsfEventRowMapper(), 
            category, limit);
        long duration = System.currentTimeMillis() - startQuery;
        
        QueryResult<OcsfEvent> result = new QueryResult<>();
        result.setResults(events);
        result.setTotal(events.size());
        result.setTookMs(duration);
        
        return result;
    }
    
    /**
     * Search events by user
     */
    public QueryResult<OcsfEvent> searchByUser(String userId, int limit) {
        String sql = """
            SELECT 
                time, category_name, class_name, severity, message,
                actor_user_uid, actor_user_name,
                src_endpoint_ip, src_endpoint_port, src_endpoint_hostname,
                dst_endpoint_ip, dst_endpoint_port, dst_endpoint_hostname,
                metadata, threat_reputation_score, threat_level,
                ueba_score, tenant_id
            FROM aegis_events_warm
            WHERE actor_user_uid = ?
            ORDER BY time DESC
            LIMIT ?
            """;
        
        long startQuery = System.currentTimeMillis();
        List<OcsfEvent> events = jdbcTemplate.query(sql, new OcsfEventRowMapper(), 
            userId, limit);
        long duration = System.currentTimeMillis() - startQuery;
        
        QueryResult<OcsfEvent> result = new QueryResult<>();
        result.setResults(events);
        result.setTotal(events.size());
        result.setTookMs(duration);
        
        return result;
    }
    
    /**
     * Row mapper for OCSF events
     */
    private static class OcsfEventRowMapper implements RowMapper<OcsfEvent> {
        @Override
        public OcsfEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
            OcsfEvent event = new OcsfEvent();
            
            event.setTime(rs.getTimestamp("time").getTime());
            event.setCategoryName(rs.getString("category_name"));
            event.setClassName(rs.getString("class_name"));
            event.setSeverity(rs.getInt("severity"));
            event.setMessage(rs.getString("message"));
            
            // Map other fields as needed
            // Simplified for now
            
            return event;
        }
    }
}
