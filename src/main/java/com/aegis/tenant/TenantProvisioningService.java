package com.aegis.tenant;

import com.aegis.domain.Tenant;
import com.aegis.domain.TenantStatus;
import com.aegis.security.TenantKeyService;
import com.aegis.storage.TenantRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Tenant provisioning service.
 *
 * Responsibilities:
 * - Create/update tenant metadata and default settings
 * - Optionally provision dedicated resources (currently Kafka topics)
 */
@Service
public class TenantProvisioningService {

    private static final Logger log = LoggerFactory.getLogger(TenantProvisioningService.class);

    // Settings keys persisted on the Tenant record
    public static final String SETTING_KAFKA_RAW_TOPIC = "kafka.topics.raw-events";
    public static final String SETTING_KAFKA_NORMALIZED_TOPIC = "kafka.topics.normalized-events";
    public static final String SETTING_KAFKA_ALERTS_TOPIC = "kafka.topics.alerts";
    public static final String SETTING_KAFKA_DLQ_TOPIC = "kafka.topics.dead-letter";
    public static final String SETTING_TENANT_MASTER_KEY_ID = "encryption.kms.masterKeyId";

    private final TenantRepository tenantRepository;
    private final ObjectProvider<AdminClient> adminClientProvider;
    private final TenantKeyService tenantKeyService;

    public TenantProvisioningService(
        TenantRepository tenantRepository,
        ObjectProvider<AdminClient> adminClientProvider,
        TenantKeyService tenantKeyService
    ) {
        this.tenantRepository = tenantRepository;
        this.adminClientProvider = adminClientProvider;
        this.tenantKeyService = tenantKeyService;
    }

    public TenantProvisioningResult provisionTenant(TenantProvisioningRequest request) {
        validateRequest(request);

        boolean dedicated = Boolean.TRUE.equals(request.getDedicatedInfrastructure());
        boolean provisionTopics = request.getProvisionKafkaTopics() != null
            ? Boolean.TRUE.equals(request.getProvisionKafkaTopics())
            : dedicated;

        TenantStatus initialStatus = request.getInitialStatus() != null
            ? request.getInitialStatus()
            : TenantStatus.PROVISIONING;

        // Build tenant with defaults
        Tenant tenant = Tenant.builder()
            .id(request.getId().trim())
            .name(request.getName().trim())
            .organization(request.getOrganization())
            .contactEmail(request.getContactEmail())
            .tier(request.getTier() != null ? request.getTier() : (dedicated ? "enterprise" : "standard"))
            .maxEps(request.getMaxEps() != null ? request.getMaxEps() : (dedicated ? 100_000L : 10_000L))
            .retentionDays(request.getRetentionDays() != null ? request.getRetentionDays() : (dedicated ? 365 : 90))
            .dedicatedInfrastructure(dedicated)
            .status(initialStatus)
            .createdAt(Instant.now())
            .updatedAt(Instant.now())
            .settings(request.getSettings() != null ? new HashMap<>(request.getSettings()) : new HashMap<>())
            .build();

        Map<String, String> resources = new HashMap<>();

        // Ensure tenant has a dedicated master key reference
        String masterKeyId = tenantKeyService.ensureTenantMasterKey(tenant.getId());
        tenant.getSettings().put(SETTING_TENANT_MASTER_KEY_ID, masterKeyId);
        resources.put("kms.masterKeyId", masterKeyId);

        if (provisionTopics) {
            Map<String, String> topics = provisionKafkaTopics(tenant);
            resources.putAll(topics);

            // Persist into tenant settings for runtime to pick up
            tenant.getSettings().put(SETTING_KAFKA_RAW_TOPIC, topics.get("kafka.rawTopic"));
            tenant.getSettings().put(SETTING_KAFKA_NORMALIZED_TOPIC, topics.get("kafka.normalizedTopic"));
            tenant.getSettings().put(SETTING_KAFKA_ALERTS_TOPIC, topics.get("kafka.alertsTopic"));
            tenant.getSettings().put(SETTING_KAFKA_DLQ_TOPIC, topics.get("kafka.deadLetterTopic"));
        }

        // Mark as active at end of provisioning (simple synchronous provisioning)
        tenant.setStatus(TenantStatus.ACTIVE);

        Tenant saved = tenantRepository.save(tenant);
        log.info("Provisioned tenant {} (dedicated={}, provisionTopics={})", saved.getId(), dedicated, provisionTopics);

        return new TenantProvisioningResult(saved, resources);
    }

    private Map<String, String> provisionKafkaTopics(Tenant tenant) {
        AdminClient adminClient = adminClientProvider.getIfAvailable();
        if (adminClient == null) {
            log.warn("AdminClient not available; skipping Kafka topic provisioning for tenant {}", tenant.getId());
            return Map.of();
        }

        String raw = kafkaTopicName(tenant.getId(), "raw-events");
        String normalized = kafkaTopicName(tenant.getId(), "normalized-events");
        String alerts = kafkaTopicName(tenant.getId(), "alerts");
        String dlq = kafkaTopicName(tenant.getId(), "dead-letter");

        try {
            // Conservative defaults; can be tuned later by tier
            short replicationFactor = 1;
            int partitions = tenant.isDedicatedInfrastructure() ? 12 : 3;

            adminClient.createTopics(
                java.util.List.of(
                    new NewTopic(raw, partitions, replicationFactor),
                    new NewTopic(normalized, partitions, replicationFactor),
                    new NewTopic(alerts, partitions, replicationFactor),
                    new NewTopic(dlq, partitions, replicationFactor)
                )
            ).all().get(10, TimeUnit.SECONDS);

            log.info("Provisioned Kafka topics for tenant {}: {}, {}, {}, {}",
                tenant.getId(), raw, normalized, alerts, dlq);

            Map<String, String> res = new HashMap<>();
            res.put("kafka.rawTopic", raw);
            res.put("kafka.normalizedTopic", normalized);
            res.put("kafka.alertsTopic", alerts);
            res.put("kafka.deadLetterTopic", dlq);
            return res;
        } catch (Exception e) {
            // It's common for topics to already exist; treat that as non-fatal for idempotency.
            // We still return the intended names so callers can store/use them.
            log.warn("Kafka topic provisioning encountered an error for tenant {} (continuing): {}",
                tenant.getId(), e.getMessage());

            Map<String, String> res = new HashMap<>();
            res.put("kafka.rawTopic", raw);
            res.put("kafka.normalizedTopic", normalized);
            res.put("kafka.alertsTopic", alerts);
            res.put("kafka.deadLetterTopic", dlq);
            return res;
        }
    }

    private String kafkaTopicName(String tenantId, String topicBaseName) {
        // Namespaced per tenant to support dedicated resources
        return "aegis." + tenantId + "." + topicBaseName;
    }

    private void validateRequest(TenantProvisioningRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        if (request.getId() == null || request.getId().trim().isEmpty()) {
            throw new IllegalArgumentException("tenant id must not be null or empty");
        }
        if (request.getName() == null || request.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("tenant name must not be null or empty");
        }
    }
}

