package com.aegis.tenant;

import com.aegis.domain.Tenant;

import java.util.Collections;
import java.util.Map;

/**
 * Result of a tenant provisioning operation.
 */
public class TenantProvisioningResult {
    private final Tenant tenant;
    private final Map<String, String> provisionedResources;

    public TenantProvisioningResult(Tenant tenant, Map<String, String> provisionedResources) {
        this.tenant = tenant;
        this.provisionedResources = provisionedResources == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(provisionedResources);
    }

    public Tenant getTenant() {
        return tenant;
    }

    /**
     * Map of provisioned resource identifiers, e.g.:
     * - kafka.rawTopic -> "aegis.tenant-acme.raw-events"
     * - kafka.normalizedTopic -> "aegis.tenant-acme.normalized-events"
     */
    public Map<String, String> getProvisionedResources() {
        return provisionedResources;
    }
}

