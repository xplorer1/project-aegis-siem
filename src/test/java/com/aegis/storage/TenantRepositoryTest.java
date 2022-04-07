package com.aegis.storage;

import com.aegis.domain.Tenant;
import com.aegis.domain.TenantStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for TenantRepository.
 * 
 * Tests cover all CRUD operations, status-based queries, and edge cases.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("TenantRepository Tests")
class TenantRepositoryTest {
    
    @Mock
    private RedisTemplate<String, String> redisTemplate;
    
    @Mock
    private ValueOperations<String, String> valueOperations;
    
    @Mock
    private SetOperations<String, String> setOperations;
    
    @InjectMocks
    private TenantRepository tenantRepository;
    
    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules(); // Register JavaTimeModule for Instant
        
        // Inject the real ObjectMapper
        tenantRepository = new TenantRepository();
        try {
            var field = TenantRepository.class.getDeclaredField("redisTemplate");
            field.setAccessible(true);
            field.set(tenantRepository, redisTemplate);
            
            field = TenantRepository.class.getDeclaredField("objectMapper");
            field.setAccessible(true);
            field.set(tenantRepository, objectMapper);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        // Setup mock operations
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
    }
    
    @Test
    @DisplayName("Should save tenant successfully")
    void shouldSaveTenant() throws Exception {
        // Given
        Tenant tenant = Tenant.builder()
            .id("tenant-123")
            .name("Test Tenant")
            .status(TenantStatus.ACTIVE)
            .tier("standard")
            .maxEps(10000)
            .retentionDays(90)
            .build();
        
        // When
        Tenant saved = tenantRepository.save(tenant);
        
        // Then
        assertThat(saved).isNotNull();
        assertThat(saved.getId()).isEqualTo("tenant-123");
        assertThat(saved.getUpdatedAt()).isNotNull();
        
        // Verify Redis operations
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations).set(keyCaptor.capture(), valueCaptor.capture());
        
        assertThat(keyCaptor.getValue()).isEqualTo("tenant:tenant-123");
        
        // Verify JSON serialization
        String json = valueCaptor.getValue();
        Tenant deserialized = objectMapper.readValue(json, Tenant.class);
        assertThat(deserialized.getId()).isEqualTo("tenant-123");
        assertThat(deserialized.getName()).isEqualTo("Test Tenant");
        
        // Verify added to all tenants set
        verify(setOperations).add("tenants:all", "tenant-123");
        
        // Verify added to status index
        verify(setOperations).add("tenants:status:active", "tenant-123");
    }
    
    @Test
    @DisplayName("Should throw exception when saving null tenant")
    void shouldThrowExceptionWhenSavingNullTenant() {
        assertThatThrownBy(() -> tenantRepository.save(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Tenant must not be null");
    }
    
    @Test
    @DisplayName("Should throw exception when saving tenant with null ID")
    void shouldThrowExceptionWhenSavingTenantWithNullId() {
        Tenant tenant = new Tenant();
        tenant.setName("Test");
        
        assertThatThrownBy(() -> tenantRepository.save(tenant))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Tenant ID must not be null or empty");
    }
    
    @Test
    @DisplayName("Should find tenant by ID")
    void shouldFindTenantById() throws Exception {
        // Given
        Tenant tenant = Tenant.builder()
            .id("tenant-123")
            .name("Test Tenant")
            .status(TenantStatus.ACTIVE)
            .build();
        
        String json = objectMapper.writeValueAsString(tenant);
        when(valueOperations.get("tenant:tenant-123")).thenReturn(json);
        
        // When
        Optional<Tenant> found = tenantRepository.findById("tenant-123");
        
        // Then
        assertThat(found).isPresent();
        assertThat(found.get().getId()).isEqualTo("tenant-123");
        assertThat(found.get().getName()).isEqualTo("Test Tenant");
        assertThat(found.get().getStatus()).isEqualTo(TenantStatus.ACTIVE);
    }
    
    @Test
    @DisplayName("Should return empty when tenant not found")
    void shouldReturnEmptyWhenTenantNotFound() {
        // Given
        when(valueOperations.get("tenant:nonexistent")).thenReturn(null);
        
        // When
        Optional<Tenant> found = tenantRepository.findById("nonexistent");
        
        // Then
        assertThat(found).isEmpty();
    }
    
    @Test
    @DisplayName("Should throw exception when finding by null ID")
    void shouldThrowExceptionWhenFindingByNullId() {
        assertThatThrownBy(() -> tenantRepository.findById(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Tenant ID must not be null or empty");
    }
    
    @Test
    @DisplayName("Should find all tenants")
    void shouldFindAllTenants() throws Exception {
        // Given
        Set<String> tenantIds = new HashSet<>(Arrays.asList("tenant-1", "tenant-2", "tenant-3"));
        when(setOperations.members("tenants:all")).thenReturn(tenantIds);
        
        Tenant tenant1 = Tenant.builder().id("tenant-1").name("Tenant 1").build();
        Tenant tenant2 = Tenant.builder().id("tenant-2").name("Tenant 2").build();
        Tenant tenant3 = Tenant.builder().id("tenant-3").name("Tenant 3").build();
        
        when(valueOperations.get("tenant:tenant-1"))
            .thenReturn(objectMapper.writeValueAsString(tenant1));
        when(valueOperations.get("tenant:tenant-2"))
            .thenReturn(objectMapper.writeValueAsString(tenant2));
        when(valueOperations.get("tenant:tenant-3"))
            .thenReturn(objectMapper.writeValueAsString(tenant3));
        
        // When
        List<Tenant> tenants = tenantRepository.findAll();
        
        // Then
        assertThat(tenants).hasSize(3);
        assertThat(tenants).extracting(Tenant::getId)
            .containsExactlyInAnyOrder("tenant-1", "tenant-2", "tenant-3");
    }
    
    @Test
    @DisplayName("Should return empty list when no tenants exist")
    void shouldReturnEmptyListWhenNoTenantsExist() {
        // Given
        when(setOperations.members("tenants:all")).thenReturn(Collections.emptySet());
        
        // When
        List<Tenant> tenants = tenantRepository.findAll();
        
        // Then
        assertThat(tenants).isEmpty();
    }
    
    @Test
    @DisplayName("Should find tenants by status")
    void shouldFindTenantsByStatus() throws Exception {
        // Given
        Set<String> activeTenantIds = new HashSet<>(Arrays.asList("tenant-1", "tenant-2"));
        when(setOperations.members("tenants:status:active")).thenReturn(activeTenantIds);
        
        Tenant tenant1 = Tenant.builder()
            .id("tenant-1")
            .name("Tenant 1")
            .status(TenantStatus.ACTIVE)
            .build();
        Tenant tenant2 = Tenant.builder()
            .id("tenant-2")
            .name("Tenant 2")
            .status(TenantStatus.ACTIVE)
            .build();
        
        when(valueOperations.get("tenant:tenant-1"))
            .thenReturn(objectMapper.writeValueAsString(tenant1));
        when(valueOperations.get("tenant:tenant-2"))
            .thenReturn(objectMapper.writeValueAsString(tenant2));
        
        // When
        List<Tenant> tenants = tenantRepository.findByStatus(TenantStatus.ACTIVE);
        
        // Then
        assertThat(tenants).hasSize(2);
        assertThat(tenants).allMatch(t -> t.getStatus() == TenantStatus.ACTIVE);
    }
    
    @Test
    @DisplayName("Should find active tenants")
    void shouldFindActiveTenants() throws Exception {
        // Given
        Set<String> activeTenantIds = new HashSet<>(Arrays.asList("tenant-1"));
        when(setOperations.members("tenants:status:active")).thenReturn(activeTenantIds);
        
        Tenant tenant1 = Tenant.builder()
            .id("tenant-1")
            .name("Active Tenant")
            .status(TenantStatus.ACTIVE)
            .build();
        
        when(valueOperations.get("tenant:tenant-1"))
            .thenReturn(objectMapper.writeValueAsString(tenant1));
        
        // When
        List<Tenant> tenants = tenantRepository.findActive();
        
        // Then
        assertThat(tenants).hasSize(1);
        assertThat(tenants.get(0).isActive()).isTrue();
    }
    
    @Test
    @DisplayName("Should throw exception when finding by null status")
    void shouldThrowExceptionWhenFindingByNullStatus() {
        assertThatThrownBy(() -> tenantRepository.findByStatus(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Status must not be null");
    }
    
    @Test
    @DisplayName("Should update existing tenant")
    void shouldUpdateExistingTenant() throws Exception {
        // Given
        Tenant existingTenant = Tenant.builder()
            .id("tenant-123")
            .name("Old Name")
            .status(TenantStatus.ACTIVE)
            .build();
        
        when(valueOperations.get("tenant:tenant-123"))
            .thenReturn(objectMapper.writeValueAsString(existingTenant));
        
        Tenant updatedTenant = Tenant.builder()
            .id("tenant-123")
            .name("New Name")
            .status(TenantStatus.SUSPENDED)
            .build();
        
        // When
        Tenant result = tenantRepository.update(updatedTenant);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo("New Name");
        
        // Verify old status index entry removed
        verify(setOperations).remove("tenants:status:active", "tenant-123");
        
        // Verify new status index entry added
        verify(setOperations).add("tenants:status:suspended", "tenant-123");
    }
    
    @Test
    @DisplayName("Should delete tenant by ID")
    void shouldDeleteTenantById() throws Exception {
        // Given
        Tenant tenant = Tenant.builder()
            .id("tenant-123")
            .name("Test Tenant")
            .status(TenantStatus.ACTIVE)
            .build();
        
        when(valueOperations.get("tenant:tenant-123"))
            .thenReturn(objectMapper.writeValueAsString(tenant));
        when(redisTemplate.delete("tenant:tenant-123")).thenReturn(true);
        
        // When
        boolean deleted = tenantRepository.deleteById("tenant-123");
        
        // Then
        assertThat(deleted).isTrue();
        
        // Verify removed from status index
        verify(setOperations).remove("tenants:status:active", "tenant-123");
        
        // Verify removed from all tenants set
        verify(setOperations).remove("tenants:all", "tenant-123");
        
        // Verify tenant record deleted
        verify(redisTemplate).delete("tenant:tenant-123");
    }
    
    @Test
    @DisplayName("Should return false when deleting non-existent tenant")
    void shouldReturnFalseWhenDeletingNonExistentTenant() {
        // Given
        when(valueOperations.get("tenant:nonexistent")).thenReturn(null);
        
        // When
        boolean deleted = tenantRepository.deleteById("nonexistent");
        
        // Then
        assertThat(deleted).isFalse();
    }
    
    @Test
    @DisplayName("Should check if tenant exists")
    void shouldCheckIfTenantExists() {
        // Given
        when(redisTemplate.hasKey("tenant:tenant-123")).thenReturn(true);
        when(redisTemplate.hasKey("tenant:nonexistent")).thenReturn(false);
        
        // When & Then
        assertThat(tenantRepository.existsById("tenant-123")).isTrue();
        assertThat(tenantRepository.existsById("nonexistent")).isFalse();
    }
    
    @Test
    @DisplayName("Should count all tenants")
    void shouldCountAllTenants() {
        // Given
        when(setOperations.size("tenants:all")).thenReturn(5L);
        
        // When
        long count = tenantRepository.count();
        
        // Then
        assertThat(count).isEqualTo(5L);
    }
    
    @Test
    @DisplayName("Should count tenants by status")
    void shouldCountTenantsByStatus() {
        // Given
        when(setOperations.size("tenants:status:active")).thenReturn(3L);
        when(setOperations.size("tenants:status:suspended")).thenReturn(2L);
        
        // When & Then
        assertThat(tenantRepository.countByStatus(TenantStatus.ACTIVE)).isEqualTo(3L);
        assertThat(tenantRepository.countByStatus(TenantStatus.SUSPENDED)).isEqualTo(2L);
    }
    
    @Test
    @DisplayName("Should handle tenant with all fields populated")
    void shouldHandleTenantWithAllFields() throws Exception {
        // Given
        Map<String, Object> settings = new HashMap<>();
        settings.put("feature_x", true);
        settings.put("threshold", 100);
        
        Tenant tenant = Tenant.builder()
            .id("tenant-full")
            .name("Full Tenant")
            .status(TenantStatus.ACTIVE)
            .tier("enterprise")
            .maxEps(50000)
            .retentionDays(365)
            .dedicatedInfrastructure(true)
            .settings(settings)
            .contactEmail("admin@example.com")
            .organization("Example Corp")
            .build();
        
        // When
        Tenant saved = tenantRepository.save(tenant);
        
        // Then
        assertThat(saved).isNotNull();
        assertThat(saved.getTier()).isEqualTo("enterprise");
        assertThat(saved.getMaxEps()).isEqualTo(50000);
        assertThat(saved.getRetentionDays()).isEqualTo(365);
        assertThat(saved.isDedicatedInfrastructure()).isTrue();
        assertThat(saved.getSettings()).containsEntry("feature_x", true);
        assertThat(saved.getContactEmail()).isEqualTo("admin@example.com");
        assertThat(saved.getOrganization()).isEqualTo("Example Corp");
    }
    
    @Test
    @DisplayName("Should update timestamp when saving")
    void shouldUpdateTimestampWhenSaving() throws Exception {
        // Given
        Instant oldTime = Instant.now().minusSeconds(3600);
        Tenant tenant = Tenant.builder()
            .id("tenant-123")
            .name("Test Tenant")
            .status(TenantStatus.ACTIVE)
            .updatedAt(oldTime)
            .build();
        
        // When
        Tenant saved = tenantRepository.save(tenant);
        
        // Then
        assertThat(saved.getUpdatedAt()).isAfter(oldTime);
    }
}
