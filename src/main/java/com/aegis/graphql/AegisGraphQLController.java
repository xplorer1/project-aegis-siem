package com.aegis.graphql;

import com.aegis.domain.Alert;
import com.aegis.domain.AlertStatus;
import com.aegis.query.AqlTranspiler;
import com.aegis.query.QueryExecutor;
import com.aegis.storage.hot.AlertRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * GraphQL controller for AEGIS SIEM API
 * 
 * This controller provides the GraphQL API interface for querying security events
 * and managing alerts. It serves as the entry point for all GraphQL operations
 * defined in the schema.graphqls file.
 * 
 * Key responsibilities:
 * - Handle GraphQL query operations (searchEvents, getAlerts)
 * - Handle GraphQL mutation operations (acknowledgeAlert)
 * - Parse and validate query arguments
 * - Coordinate with backend services (QueryExecutor, AlertRepository)
 * - Transform results to GraphQL response format
 * 
 * The controller uses Spring GraphQL annotations:
 * - @Controller: Marks this as a GraphQL controller
 * - @QueryMapping: Maps methods to GraphQL Query type fields
 * - @MutationMapping: Maps methods to GraphQL Mutation type fields
 * - @Argument: Binds method parameters to GraphQL arguments
 */
@Controller
public class AegisGraphQLController {
    private static final Logger logger = LoggerFactory.getLogger(AegisGraphQLController.class);
    
    // Pattern for parsing relative time ranges like "last 24h", "last 7d"
    private static final Pattern TIME_RANGE_PATTERN = Pattern.compile("last\\s+(\\d+)([hdwmy])");
    
    /**
     * Repository for querying and managing alerts in OpenSearch
     */
    @Autowired
    private AlertRepository alertRepository;
    
    /**
     * Transpiler for converting AQL queries to tier-specific query formats
     * Used by searchEvents to execute queries across Hot/Warm/Cold tiers
     */
    @Autowired
    private AqlTranspiler aqlTranspiler;
    
    /**
     * Executor for running queries across multiple storage tiers
     * Handles concurrent execution and result merging
     */
    @Autowired
    private QueryExecutor queryExecutor;
    
    /**
     * Query alerts with optional filters
     * 
     * @param severity Optional severity filter (1=Info, 2=Low, 3=Medium, 4=High, 5=Critical)
     * @param status Optional status filter
     * @param timeRange Time range in AQL format (e.g., "last 24h", "last 7d")
     * @param limit Maximum number of results (default: 100)
     * @param offset Offset for pagination (default: 0)
     * @return List of alerts matching the criteria
     */
    @QueryMapping
    public List<Alert> getAlerts(
            @Argument Integer severity,
            @Argument String status,
            @Argument String timeRange,
            @Argument Integer limit,
            @Argument Integer offset) {
        
        logger.debug("getAlerts called with severity={}, status={}, timeRange={}, limit={}, offset={}",
            severity, status, timeRange, limit, offset);
        
        // Parse time range
        TimeRangeBounds bounds = parseTimeRange(timeRange);
        
        // Parse status if provided
        AlertStatus alertStatus = null;
        if (status != null && !status.isEmpty()) {
            try {
                alertStatus = AlertStatus.valueOf(status.toUpperCase());
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid alert status: {}", status);
                throw new IllegalArgumentException("Invalid alert status: " + status);
            }
        }
        
        // Set defaults
        int resultLimit = (limit != null && limit > 0) ? Math.min(limit, 10000) : 100;
        int resultOffset = (offset != null && offset >= 0) ? offset : 0;
        
        // Query alerts
        List<Alert> alerts = alertRepository.findAlerts(
            severity,
            alertStatus,
            bounds.startTime,
            bounds.endTime,
            resultLimit,
            resultOffset
        );
        
        logger.info("Returning {} alerts", alerts.size());
        return alerts;
    }
    
    /**
     * Parse time range string to start and end timestamps
     * Supports formats like:
     * - "last 24h" (last 24 hours)
     * - "last 7d" (last 7 days)
     * - "last 4w" (last 4 weeks)
     * - "last 3m" (last 3 months)
     * - "last 1y" (last 1 year)
     * 
     * @param timeRange Time range string
     * @return TimeRangeBounds with start and end timestamps in epoch millis
     */
    private TimeRangeBounds parseTimeRange(String timeRange) {
        if (timeRange == null || timeRange.isEmpty()) {
            throw new IllegalArgumentException("timeRange is required");
        }
        
        Matcher matcher = TIME_RANGE_PATTERN.matcher(timeRange.toLowerCase().trim());
        
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                "Invalid time range format. Expected format: 'last Nh' where N is a number and unit is h/d/w/m/y");
        }
        
        int amount = Integer.parseInt(matcher.group(1));
        String unit = matcher.group(2);
        
        Instant endTime = Instant.now();
        Instant startTime;
        
        switch (unit) {
            case "h":
                startTime = endTime.minus(amount, ChronoUnit.HOURS);
                break;
            case "d":
                startTime = endTime.minus(amount, ChronoUnit.DAYS);
                break;
            case "w":
                startTime = endTime.minus(amount * 7, ChronoUnit.DAYS);
                break;
            case "m":
                startTime = endTime.minus(amount * 30, ChronoUnit.DAYS);
                break;
            case "y":
                startTime = endTime.minus(amount * 365, ChronoUnit.DAYS);
                break;
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }
        
        logger.debug("Parsed time range '{}' to startTime={}, endTime={}", 
            timeRange, startTime, endTime);
        
        return new TimeRangeBounds(
            startTime.toEpochMilli(),
            endTime.toEpochMilli()
        );
    }
    
    /**
     * Helper class to hold time range bounds
     */
    private static class TimeRangeBounds {
        final long startTime;
        final long endTime;
        
        TimeRangeBounds(long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }
}
