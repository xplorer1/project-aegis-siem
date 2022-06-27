package com.aegis.graphql;

import com.aegis.domain.Alert;
import com.aegis.domain.AlertStatus;
import com.aegis.domain.OcsfEvent;
import com.aegis.query.AqlTranspiler;
import com.aegis.query.QueryExecutor;
import com.aegis.security.TenantContext;
import com.aegis.security.TenantValidator;
import com.aegis.storage.hot.AlertRepository;
import org.dataloader.DataLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SchemaMapping;
import org.springframework.graphql.execution.BatchLoaderRegistry;
import org.springframework.stereotype.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    private final AlertRepository alertRepository;
    
    /**
     * Transpiler for converting AQL queries to tier-specific query formats
     * Used by searchEvents to execute queries across Hot/Warm/Cold tiers
     */
    private final AqlTranspiler aqlTranspiler;
    
    /**
     * Executor for running queries across multiple storage tiers
     * Handles concurrent execution and result merging
     */
    private final QueryExecutor queryExecutor;
    
    /**
     * Validator for enforcing multi-tenant isolation
     * Ensures tenant_id in requests matches authenticated tenant_id
     */
    private final TenantValidator tenantValidator;
    
    /**
     * Constructor with dependency injection
     * 
     * @param alertRepository Repository for alert operations
     * @param aqlTranspiler AQL query transpiler
     * @param queryExecutor Query executor for multi-tier queries
     * @param tenantValidator Validator for tenant access control
     */
    @Autowired
    public AegisGraphQLController(
            AlertRepository alertRepository,
            AqlTranspiler aqlTranspiler,
            QueryExecutor queryExecutor,
            TenantValidator tenantValidator) {
        this.alertRepository = alertRepository;
        this.aqlTranspiler = aqlTranspiler;
        this.queryExecutor = queryExecutor;
        this.tenantValidator = tenantValidator;
    }
    
    /**
     * Query alerts with optional filters
     * 
     * This method enforces tenant isolation by validating that the authenticated
     * tenant (from JWT token) matches the tenant of the requested alerts.
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
        
        // Get authenticated tenant from context
        // The TenantInterceptor has already validated the JWT and set the tenant context
        String authenticatedTenantId = tenantValidator.getAuthenticatedTenantId();
        
        logger.debug("Authenticated tenant: {}", authenticatedTenantId);
        
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
        
        // Query alerts - the repository will automatically filter by tenant_id
        // using the TenantContext set by TenantInterceptor
        List<Alert> alerts = alertRepository.findAlerts(
            severity,
            alertStatus,
            bounds.startTime,
            bounds.endTime,
            resultLimit,
            resultOffset
        );
        
        // Validate that all returned alerts belong to the authenticated tenant
        // This is a defense-in-depth measure to catch any bugs in the repository layer
        for (Alert alert : alerts) {
            if (alert.getTenantId() != null && !alert.getTenantId().equals(authenticatedTenantId)) {
                logger.error("Security violation: Alert {} belongs to tenant {} but authenticated tenant is {}",
                    alert.getId(), alert.getTenantId(), authenticatedTenantId);
                throw new IllegalStateException("Internal error: tenant isolation violation detected");
            }
        }
        
        logger.info("Returning {} alerts for tenant {}", alerts.size(), authenticatedTenantId);
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
    
    /**
     * Acknowledge an alert
     * Updates the alert status to ACKNOWLEDGED and records the acknowledgment timestamp
     * 
     * This method enforces tenant isolation by validating that the alert being
     * acknowledged belongs to the authenticated tenant.
     * 
     * @param alertId The unique identifier of the alert to acknowledge
     * @return The updated alert
     */
    @MutationMapping
    public Alert acknowledgeAlert(@Argument String alertId) {
        logger.debug("acknowledgeAlert called with alertId={}", alertId);
        
        if (alertId == null || alertId.isEmpty()) {
            throw new IllegalArgumentException("alertId is required");
        }
        
        // Get authenticated tenant from context
        String authenticatedTenantId = tenantValidator.getAuthenticatedTenantId();
        
        logger.debug("Authenticated tenant: {}", authenticatedTenantId);
        
        // Find the alert
        Alert alert = alertRepository.findById(alertId);
        
        if (alert == null) {
            logger.warn("Alert not found: {}", alertId);
            throw new IllegalArgumentException("Alert not found: " + alertId);
        }
        
        // Validate tenant access - ensure the alert belongs to the authenticated tenant
        // This enforces Requirement 9.6: tenant_id validation
        if (alert.getTenantId() != null) {
            tenantValidator.validateTenantAccess(alert.getTenantId());
            logger.debug("Tenant validation successful for alert {}", alertId);
        } else {
            logger.warn("Alert {} has no tenant_id set", alertId);
        }
        
        // Update alert status
        alert.setStatus(AlertStatus.ACKNOWLEDGED);
        alert.setAcknowledgedAt(Instant.now());
        
        // Save the updated alert
        Alert updatedAlert = alertRepository.save(alert);
        
        logger.info("Alert {} acknowledged successfully by tenant {}", alertId, authenticatedTenantId);
        return updatedAlert;
    }
    
    /**
     * Field resolver for Alert.events
     * 
     * This method is called by GraphQL when the 'events' field is requested on an Alert.
     * It uses DataLoader to batch fetch events, preventing N+1 query problems.
     * 
     * Without DataLoader:
     *   - Fetching 10 alerts with 5 events each = 1 + (10 * 5) = 51 queries
     * 
     * With DataLoader:
     *   - Fetching 10 alerts with 5 events each = 1 + 1 = 2 queries
     *   - All event IDs are collected and fetched in a single batch
     * 
     * @param alert The parent Alert object
     * @param dataLoader The DataLoader for batching event fetches
     * @return CompletableFuture containing the list of events
     */
    @SchemaMapping(typeName = "Alert", field = "events")
    public CompletableFuture<List<OcsfEvent>> getAlertEvents(
            Alert alert,
            DataLoader<String, OcsfEvent> dataLoader) {
        
        logger.debug("Resolving events for alert {} with {} event IDs", 
            alert.getId(), alert.getEventIds() != null ? alert.getEventIds().size() : 0);
        
        // If no event IDs, return empty list
        if (alert.getEventIds() == null || alert.getEventIds().isEmpty()) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }
        
        // Load all events using DataLoader
        // DataLoader will batch these requests and fetch all events in a single query
        List<CompletableFuture<OcsfEvent>> eventFutures = new ArrayList<>();
        for (String eventId : alert.getEventIds()) {
            eventFutures.add(dataLoader.load(eventId));
        }
        
        // Combine all futures into a single future that returns a list
        return CompletableFuture.allOf(eventFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                List<OcsfEvent> events = new ArrayList<>();
                for (CompletableFuture<OcsfEvent> future : eventFutures) {
                    OcsfEvent event = future.join();
                    if (event != null) {
                        events.add(event);
                    }
                }
                logger.debug("Resolved {} events for alert {}", events.size(), alert.getId());
                return events;
            });
    }
}
