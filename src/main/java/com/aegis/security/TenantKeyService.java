package com.aegis.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

/**
 * Tenant-specific encryption key service.
 *
 * This is a lightweight "KMS-like" abstraction for development:
 * - Generates a unique master key per tenant
 * - Stores key material in Redis (in production this would be an external KMS)
 * - Returns a keyId reference that can be stored on the Tenant settings
 */
@Service
public class TenantKeyService {

    private static final Logger log = LoggerFactory.getLogger(TenantKeyService.class);

    private static final String TENANT_ALIAS_PREFIX = "kms:alias:tenant:";
    private static final String KEY_MATERIAL_PREFIX = "kms:key:";

    private final RedisTemplate<String, String> redisTemplate;
    private final SecureRandom secureRandom = new SecureRandom();

    public TenantKeyService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Ensure a tenant has a master key and return its keyId.
     *
     * Idempotent: if a key already exists for the tenant alias, it is returned.
     */
    public String ensureTenantMasterKey(String tenantId) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        String trimmed = tenantId.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("tenantId must not be empty");
        }

        String aliasKey = TENANT_ALIAS_PREFIX + trimmed;
        String existingKeyId = redisTemplate.opsForValue().get(aliasKey);
        if (existingKeyId != null && !existingKeyId.isBlank()) {
            log.debug("Found existing tenant master key for {}: {}", trimmed, existingKeyId);
            return existingKeyId;
        }

        String keyId = "key-" + UUID.randomUUID();
        String keyMaterial = generateAes256KeyMaterial();

        // Store key material and alias mapping. No TTL (persistent) like a KMS key reference.
        redisTemplate.opsForValue().set(KEY_MATERIAL_PREFIX + keyId, keyMaterial);
        redisTemplate.opsForValue().set(aliasKey, keyId);

        log.info("Generated tenant master key for {}: {}", trimmed, keyId);
        return keyId;
    }

    /**
     * Retrieve key material for a keyId.
     *
     * Note: Callers should avoid using raw key material directly in production.
     */
    public String getKeyMaterial(String keyId) {
        Objects.requireNonNull(keyId, "keyId must not be null");
        String material = redisTemplate.opsForValue().get(KEY_MATERIAL_PREFIX + keyId);
        if (material == null) {
            throw new IllegalArgumentException("Unknown keyId: " + keyId);
        }
        return material;
    }

    /**
     * Generate a new data key (DEK) and return the base64 key material.
     *
     * This is a placeholder for envelope encryption (Task 22.2).
     */
    public String generateDataKey(String keyId, Duration ttl) {
        Objects.requireNonNull(keyId, "keyId must not be null");
        // validate key exists
        getKeyMaterial(keyId);

        String dek = generateAes256KeyMaterial();
        if (ttl != null && !ttl.isZero() && !ttl.isNegative()) {
            redisTemplate.opsForValue().set(KEY_MATERIAL_PREFIX + keyId + ":dek:" + UUID.randomUUID(), dek, ttl);
        }
        return dek;
    }

    private String generateAes256KeyMaterial() {
        byte[] bytes = new byte[32]; // 256-bit
        secureRandom.nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }
}

