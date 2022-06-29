package com.aegis.graphql;

import com.aegis.domain.Alert;
import com.aegis.domain.AlertStatus;
import com.aegis.security.TenantContext;
import com.aegis.security.TenantValidator;
import com.aegis.storage.hot.AlertRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.graphql.test.tester.GraphQlTester;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for tenant validation in GraphQL API
 * 
 * Tests Requirement 9.6: "THE API_Gateway SHALL validate tenant_id in 
 * authentication tokens matches tenant_id in all API requests"
 * 
 * These tests verify that:
 * 1. Authenticated tenants can only access their own data
 * 2. Cross-tenant access attempts are rejected
 * 3. Proper error messages are returned for validation failures
 */
@SpringBootTest
@DisplayName("GraphQL Tenant Validation Integration Tests")
class TenantValidationIntegrationTest {
    
    @Autowired
    private GraphQlTester graphQlTester;
    
    @MockBean
    private AlertRepository alertRepository;
    
    @Autowired
    private TenantValidator tenantValidator;
    
    private static final String TENANT_1 = "tenant-123";
    private static final String TENANT_2 = "tenant-456";
    
    @BeforeEach
    void setUp() {
        TenantContext.clear();
    }
    
    @AfterEach
    void tearDown() {
        TenantContext.clear();
    }
    
    @Test
    @DisplayName("getAlerts should return alerts for authenticated tenant")
    void getAlertsShouldReturnAlertsForAuthenticatedTenant() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        Alert alert1 = createAlert("alert-1", TENANT_1, AlertStatus.OPEN, 4);
        Alert alert2 = createAlert("alert-2", TENANT_1, AlertStatus.ACKNOWLEDGED, 3);
        
        when(alertRepository.findAlerts(anyInt(), any(), anyLong(), anyLong(), anyInt(), anyInt()))
            .thenReturn(Arrays.asList(alert1, alert2));
        
        // When/Then
        graphQlTester.document("""
            query {
                getAlerts(severity: 3, timeRange: "last 24h", limit: 100, offset: 0) {
                    id
                    tenantId
                    severity
                    status
                }
            }
            """)
            .execute()
            .path("getAlerts")
            .entityList(Alert.class)
            .hasSize(2)
            .contains(alert1, alert2);
        
        verify(alertRepository).findAlerts(eq(3), isNull(), anyLong(), anyLong(), eq(100), eq(0));
    }
    
    @Test
    @DisplayName("getAlerts should reject alerts from different tenant")
    void getAlertsShouldRejectAlertsFromDifferentTenant() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        // Repository returns an alert from a different tenant (simulating a bug)
        Alert alert = createAlert("alert-1", TENANT_2, AlertStatus.OPEN, 4);
        
        when(alertRepository.findAlerts(anyInt(), any(), anyLong(), anyLong(), anyInt(), anyInt()))
            .thenReturn(Arrays.asList(alert));
        
        // When/Then - should throw exception due to tenant mismatch
        graphQlTester.document("""
            query {
                getAlerts(severity: 4, timeRange: "last 24h", limit: 100, offset: 0) {
                    id
                    tenantId
                    severity
                }
            }
            """)
            .execute()
            .errors()
            .expect(error -> error.getMessage().contains("tenant isolation violation"));
    }
    
    @Test
    @DisplayName("acknowledgeAlert should succeed for own tenant's alert")
    void acknowledgeAlertShouldSucceedForOwnTenantsAlert() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        Alert alert = createAlert("alert-1", TENANT_1, AlertStatus.OPEN, 4);
        Alert acknowledgedAlert = createAlert("alert-1", TENANT_1, AlertStatus.ACKNOWLEDGED, 4);
        acknowledgedAlert.setAcknowledgedAt(Instant.now());
        
        when(alertRepository.findById("alert-1")).thenReturn(alert);
        when(alertRepository.save(any(Alert.class))).thenReturn(acknowledgedAlert);
        
        // When/Then
        graphQlTester.document("""
            mutation {
                acknowledgeAlert(alertId: "alert-1") {
                    id
                    status
                    acknowledgedAt
                }
            }
            """)
            .execute()
            .path("acknowledgeAlert.id").entity(String.class).isEqualTo("alert-1")
            .path("acknowledgeAlert.status").entity(String.class).isEqualTo("ACKNOWLEDGED");
        
        verify(alertRepository).findById("alert-1");
        verify(alertRepository).save(any(Alert.class));
    }
    
    @Test
    @DisplayName("acknowledgeAlert should reject alert from different tenant")
    void acknowledgeAlertShouldRejectAlertFromDifferentTenant() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        // Alert belongs to a different tenant
        Alert alert = createAlert("alert-1", TENANT_2, AlertStatus.OPEN, 4);
        
        when(alertRepository.findById("alert-1")).thenReturn(alert);
        
        // When/Then - should throw TenantAccessDeniedException
        graphQlTester.document("""
            mutation {
                acknowledgeAlert(alertId: "alert-1") {
                    id
                    status
                }
            }
            """)
            .execute()
            .errors()
            .expect(error -> error.getMessage().contains("Access denied"))
            .expect(error -> error.getExtensions().get("errorCode").equals("TENANT_ACCESS_DENIED"));
        
        verify(alertRepository).findById("alert-1");
        verify(alertRepository, never()).save(any(Alert.class));
    }
    
    @Test
    @DisplayName("acknowledgeAlert should handle alert without tenant ID")
    void acknowledgeAlertShouldHandleAlertWithoutTenantId() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        // Alert has no tenant ID (legacy data or bug)
        Alert alert = createAlert("alert-1", null, AlertStatus.OPEN, 4);
        Alert acknowledgedAlert = createAlert("alert-1", null, AlertStatus.ACKNOWLEDGED, 4);
        acknowledgedAlert.setAcknowledgedAt(Instant.now());
        
        when(alertRepository.findById("alert-1")).thenReturn(alert);
        when(alertRepository.save(any(Alert.class))).thenReturn(acknowledgedAlert);
        
        // When/Then - should succeed (validation is skipped for null tenant ID)
        graphQlTester.document("""
            mutation {
                acknowledgeAlert(alertId: "alert-1") {
                    id
                    status
                }
            }
            """)
            .execute()
            .path("acknowledgeAlert.id").entity(String.class).isEqualTo("alert-1")
            .path("acknowledgeAlert.status").entity(String.class).isEqualTo("ACKNOWLEDGED");
    }
    
    @Test
    @DisplayName("getAlerts should fail when no tenant is authenticated")
    void getAlertsShouldFailWhenNoTenantAuthenticated() {
        // Given - no tenant set in context
        
        // When/Then
        graphQlTester.document("""
            query {
                getAlerts(severity: 4, timeRange: "last 24h", limit: 100, offset: 0) {
                    id
                }
            }
            """)
            .execute()
            .errors()
            .expect(error -> error.getMessage().contains("No authenticated tenant"));
    }
    
    @Test
    @DisplayName("acknowledgeAlert should fail when no tenant is authenticated")
    void acknowledgeAlertShouldFailWhenNoTenantAuthenticated() {
        // Given - no tenant set in context
        
        // When/Then
        graphQlTester.document("""
            mutation {
                acknowledgeAlert(alertId: "alert-1") {
                    id
                }
            }
            """)
            .execute()
            .errors()
            .expect(error -> error.getMessage().contains("No authenticated tenant"));
    }
    
    @Test
    @DisplayName("getAlerts should handle empty result set")
    void getAlertsShouldHandleEmptyResultSet() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        when(alertRepository.findAlerts(anyInt(), any(), anyLong(), anyLong(), anyInt(), anyInt()))
            .thenReturn(Arrays.asList());
        
        // When/Then
        graphQlTester.document("""
            query {
                getAlerts(severity: 5, timeRange: "last 1h", limit: 100, offset: 0) {
                    id
                }
            }
            """)
            .execute()
            .path("getAlerts")
            .entityList(Alert.class)
            .hasSize(0);
    }
    
    @Test
    @DisplayName("acknowledgeAlert should fail for non-existent alert")
    void acknowledgeAlertShouldFailForNonExistentAlert() {
        // Given
        TenantContext.setTenantId(TENANT_1);
        
        when(alertRepository.findById("non-existent")).thenReturn(null);
        
        // When/Then
        graphQlTester.document("""
            mutation {
                acknowledgeAlert(alertId: "non-existent") {
                    id
                }
            }
            """)
            .execute()
            .errors()
            .expect(error -> error.getMessage().contains("Alert not found"));
    }
    
    /**
     * Helper method to create test alerts
     */
    private Alert createAlert(String id, String tenantId, AlertStatus status, int severity) {
        Alert alert = new Alert();
        alert.setId(id);
        alert.setTenantId(tenantId);
        alert.setStatus(status);
        alert.setSeverity(severity);
        alert.setTitle("Test Alert");
        alert.setDescription("Test alert description");
        alert.setRuleId("rule-1");
        alert.setCreatedAt(Instant.now());
        alert.setEventIds(Arrays.asList("event-1", "event-2"));
        return alert;
    }
}
