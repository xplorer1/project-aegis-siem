package com.aegis.tenant;

import com.aegis.domain.Tenant;
import com.aegis.storage.TenantRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

/**
 * Simple control-plane API for tenant provisioning and lookup.
 *
 * Note: authn/authz is expected to be enforced by upstream security layers.
 */
@RestController
@RequestMapping("/api/tenants")
public class TenantProvisioningController {

    private final TenantProvisioningService provisioningService;
    private final TenantRepository tenantRepository;

    public TenantProvisioningController(
        TenantProvisioningService provisioningService,
        TenantRepository tenantRepository
    ) {
        this.provisioningService = provisioningService;
        this.tenantRepository = tenantRepository;
    }

    @PostMapping("/provision")
    public ResponseEntity<TenantProvisioningResult> provision(@RequestBody TenantProvisioningRequest request) {
        TenantProvisioningResult result = provisioningService.provisionTenant(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(result);
    }

    @GetMapping
    public List<Tenant> listTenants() {
        return tenantRepository.findAll();
    }

    @GetMapping("/{tenantId}")
    public ResponseEntity<Tenant> getTenant(@PathVariable String tenantId) {
        Optional<Tenant> tenant = tenantRepository.findById(tenantId);
        return tenant.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }
}

