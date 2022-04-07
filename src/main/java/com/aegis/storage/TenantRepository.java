package com.aegis.storage;

import com.aegis.domain.Tenant;
import com.aegis.domain.TenantStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Repository for managing tenant metadata in Redis.
 * 
 * This repository provides CRUD operations for tenant management and supports
 * querying tenants by various criteria. Tenant metadata is stored in Redis
 * for fast access during request processing and tenant validation.
 * 
 * Key Design Decisions:
 * - Uses Redis for low-latency access during request processing
 * - Stores tenant data as JSON for flexibility
 * - No TTL on tenant records (they persist until explicitly deleted)
 * - Supports secondary indices for status-based queries
 * 
 * @see Tenant
 * @see TenantStatus
 */
@Repository
public class TenantRepository {
    
    private static final Logger log = LoggerFactory.getLogger(TenantRepository.class);
    
    /**
     * Redis key prefix for tenant records
     */
    private static final String KEY_PREFIX = "tenant:";
    
    /**
     * Redis key for the set of all tenant IDs
     */
    private static final String ALL_TENANTS_KEY = "tenants:all";
    
    /**
     * Redis key prefix for tenant status indices
     */
    private static final String STATUS_INDEX_PREFIX = "tenants:status:";
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    /**
     * Save a tenant to the repository.
     * 
     * If the tenant already exists, it will be updated. The updatedAt
     * timestamp is automatically set to the current time.
     * 
     * @param tenant The tenant to save
     * @return The saved tenant
     * @throws IllegalArgumentException if tenant or tenant ID is null
     * @throws RuntimeException if save operation fails
     */
    public Tenant save(Tenant tenant) {
        if (tenant == null) {
            throw new IllegalArgumentException("Tenant must not be null");
        }
        if (tenant.getId() == null || tenant.getId().trim().isEmpty()) {
            throw new IllegalArgumentException("Tenant ID must not be null or empty");
        }
        
        try {
            // Update timestamp
            tenant.touch();
            
            // Serialize to JSON
            String key = KEY_PREFIX + tenant.getId();
            String json = objectMapper.writeValueAsString(tenant);
            
            // Save to Redis (no TTL - tenants persist until deleted)
            redisTemplate.opsForValue().set(key, json);
            
            // Add to all tenants set
            redisTemplate.opsForSet().add(ALL_TENANTS_KEY, tenant.getId());
            
            // Update status index
            updateStatusIndex(tenant);
            
            log.info("Saved tenant: id={}, name={}, status={}", 
                tenant.getId(), tenant.getName(), tenant.getStatus());
            
            return tenant;
            
        } catch (Exception e) {
            log.error("Failed to save tenant: {}", tenant.getId(), e);
            throw new RuntimeException("Failed to save tenant", e);
        }
    }
    
    /**
     * Find a tenant by ID.
     * 
     * @param tenantId The tenant ID
     * @return Optional containing the tenant if found, empty otherwise
     * @throws IllegalArgumentException if tenantId is null or empty
     */
    public Optional<Tenant> findById(String tenantId) {
        if (tenantId == null || tenantId.trim().isEmpty()) {
            throw new IllegalArgumentException("Tenant ID must not be null or empty");
        }
        
        try {
            String key = KEY_PREFIX + tenantId;
            String json = redisTemplate.opsForValue().get(key);
            
            if (json == null) {
                log.debug("Tenant not found: {}", tenantId);
                return Optional.empty();
            }
            
            Tenant tenant = objectMapper.readValue(json, Tenant.class);
            log.debug("Found tenant: id={}, name={}", tenant.getId(), tenant.getName());
            
            return Optional.of(tenant);
            
        } catch (Exception e) {
            log.error("Failed to find tenant: {}", tenantId, e);
            return Optional.empty();
        }
    }
    
    /**
     * Find all tenants in the system.
     * 
     * @return List of all tenants
     */
    public List<Tenant> findAll() {
        try {
            Set<String> tenantIds = redisTemplate.opsForSet().members(ALL_TENANTS_KEY);
            
            if (tenantIds == null || tenantIds.isEmpty()) {
                log.debug("No tenants found");
                return new ArrayList<>();
            }
            
            List<Tenant> tenants = new ArrayList<>();
            for (String tenantId : tenantIds) {
                findById(tenantId).ifPresent(tenants::add);
            }
            
            log.debug("Found {} tenants", tenants.size());
            return tenants;
            
        } catch (Exception e) {
            log.error("Failed to find all tenants", e);
            return new ArrayList<>();
        }
    }
    
    /**
     * Find tenants by status.
     * 
     * This method uses a secondary index to efficiently query tenants
     * by their status without scanning all tenant records.
     * 
     * @param status The tenant status to filter by
     * @return List of tenants with the specified status
     * @throws IllegalArgumentException if status is null
     */
    public List<Tenant> findByStatus(TenantStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("Status must not be null");
        }
        
        try {
            String indexKey = STATUS_INDEX_PREFIX + status.getValue();
            Set<String> tenantIds = redisTemplate.opsForSet().members(indexKey);
            
            if (tenantIds == null || tenantIds.isEmpty()) {
                log.debug("No tenants found with status: {}", status);
                return new ArrayList<>();
            }
            
            List<Tenant> tenants = new ArrayList<>();
            for (String tenantId : tenantIds) {
                findById(tenantId).ifPresent(tenants::add);
            }
            
            log.debug("Found {} tenants with status: {}", tenants.size(), status);
            return tenants;
            
        } catch (Exception e) {
            log.error("Failed to find tenants by status: {}", status, e);
            return new ArrayList<>();
        }
    }
    
    /**
     * Find all active tenants.
     * 
     * This is a convenience method that calls findByStatus(TenantStatus.ACTIVE).
     * 
     * @return List of active tenants
     */
    public List<Tenant> findActive() {
        return findByStatus(TenantStatus.ACTIVE);
    }
    
    /**
     * Update an existing tenant.
     * 
     * This method updates the tenant and automatically sets the updatedAt
     * timestamp. If the tenant doesn't exist, it will be created.
     * 
     * @param tenant The tenant to update
     * @return The updated tenant
     * @throws IllegalArgumentException if tenant or tenant ID is null
     */
    public Tenant update(Tenant tenant) {
        if (tenant == null) {
            throw new IllegalArgumentException("Tenant must not be null");
        }
        if (tenant.getId() == null || tenant.getId().trim().isEmpty()) {
            throw new IllegalArgumentException("Tenant ID must not be null or empty");
        }
        
        // Check if tenant exists
        Optional<Tenant> existing = findById(tenant.getId());
        if (existing.isEmpty()) {
            log.warn("Tenant not found for update, creating new: {}", tenant.getId());
        } else {
            // Remove old status index entry if status changed
            Tenant oldTenant = existing.get();
            if (oldTenant.getStatus() != tenant.getStatus()) {
                removeFromStatusIndex(oldTenant);
            }
        }
        
        // Save (which handles index updates)
        return save(tenant);
    }
    
    /**
     * Delete a tenant by ID.
     * 
     * This method removes the tenant record and all associated index entries.
     * 
     * @param tenantId The tenant ID to delete
     * @return true if the tenant was deleted, false if it didn't exist
     * @throws IllegalArgumentException if tenantId is null or empty
     */
    public boolean deleteById(String tenantId) {
        if (tenantId == null || tenantId.trim().isEmpty()) {
            throw new IllegalArgumentException("Tenant ID must not be null or empty");
        }
        
        try {
            // Get tenant to remove from status index
            Optional<Tenant> tenant = findById(tenantId);
            
            if (tenant.isEmpty()) {
                log.debug("Tenant not found for deletion: {}", tenantId);
                return false;
            }
            
            // Remove from status index
            removeFromStatusIndex(tenant.get());
            
            // Remove from all tenants set
            redisTemplate.opsForSet().remove(ALL_TENANTS_KEY, tenantId);
            
            // Delete tenant record
            String key = KEY_PREFIX + tenantId;
            Boolean deleted = redisTemplate.delete(key);
            
            log.info("Deleted tenant: {}", tenantId);
            return Boolean.TRUE.equals(deleted);
            
        } catch (Exception e) {
            log.error("Failed to delete tenant: {}", tenantId, e);
            return false;
        }
    }
    
    /**
     * Check if a tenant exists.
     * 
     * @param tenantId The tenant ID to check
     * @return true if the tenant exists, false otherwise
     * @throws IllegalArgumentException if tenantId is null or empty
     */
    public boolean existsById(String tenantId) {
        if (tenantId == null || tenantId.trim().isEmpty()) {
            throw new IllegalArgumentException("Tenant ID must not be null or empty");
        }
        
        String key = KEY_PREFIX + tenantId;
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }
    
    /**
     * Count all tenants in the system.
     * 
     * @return Total number of tenants
     */
    public long count() {
        Long size = redisTemplate.opsForSet().size(ALL_TENANTS_KEY);
        return size != null ? size : 0L;
    }
    
    /**
     * Count tenants by status.
     * 
     * @param status The tenant status to count
     * @return Number of tenants with the specified status
     * @throws IllegalArgumentException if status is null
     */
    public long countByStatus(TenantStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("Status must not be null");
        }
        
        String indexKey = STATUS_INDEX_PREFIX + status.getValue();
        Long size = redisTemplate.opsForSet().size(indexKey);
        return size != null ? size : 0L;
    }
    
    /**
     * Update the status index for a tenant.
     * 
     * This method adds the tenant ID to the appropriate status index set.
     * 
     * @param tenant The tenant to index
     */
    private void updateStatusIndex(Tenant tenant) {
        if (tenant.getStatus() != null) {
            String indexKey = STATUS_INDEX_PREFIX + tenant.getStatus().getValue();
            redisTemplate.opsForSet().add(indexKey, tenant.getId());
        }
    }
    
    /**
     * Remove a tenant from its status index.
     * 
     * @param tenant The tenant to remove from index
     */
    private void removeFromStatusIndex(Tenant tenant) {
        if (tenant.getStatus() != null) {
            String indexKey = STATUS_INDEX_PREFIX + tenant.getStatus().getValue();
            redisTemplate.opsForSet().remove(indexKey, tenant.getId());
        }
    }
}
