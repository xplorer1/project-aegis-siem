package com.aegis.graphql;

import com.aegis.domain.Alert;
import com.aegis.domain.AlertStatus;
import com.aegis.query.AqlTranspiler;
import com.aegis.query.QueryExecutor;
import com.aegis.storage.hot.AlertRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for acknowledgeAlert mutation in AegisGraphQLController
 */
@ExtendWith(MockitoExtension.class)
class AcknowledgeAlertMutationTest {
    
    @Mock
    private AqlTranspiler aqlTranspiler;
    
    @Mock
    private QueryExecutor queryExecutor;
    
    @Mock
    private AlertRepository alertRepository;
    
    private AegisGraphQLController controller;
    
    @BeforeEach
    void setUp() {
        controller = new AegisGraphQLController(aqlTranspiler, queryExecutor, alertRepository);
    }
    
    @Test
    void testAcknowledgeAlert_Success() {
        // Arrange
        String alertId = "alert-123";
        
        Alert mockAlert = new Alert();
        mockAlert.setId(alertId);
        mockAlert.setStatus(AlertStatus.OPEN);
        mockAlert.setTitle("Test Alert");
        mockAlert.setSeverity(4);
        
        when(alertRepository.findById(alertId)).thenReturn(mockAlert);
        when(alertRepository.save(any(Alert.class))).thenAnswer(invocation -> {
            Alert alert = invocation.getArgument(0);
            return alert;
        });
        
        // Act
        Alert result = controller.acknowledgeAlert(alertId);
        
        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(alertId);
        assertThat(result.getStatus()).isEqualTo(AlertStatus.ACKNOWLEDGED);
        assertThat(result.getAcknowledgedAt()).isNotNull();
        
        verify(alertRepository, times(1)).findById(alertId);
        verify(alertRepository, times(1)).save(any(Alert.class));
    }
    
    @Test
    void testAcknowledgeAlert_NullAlertId() {
        // Act & Assert
        assertThatThrownBy(() -> controller.acknowledgeAlert(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("alertId is required");
        
        verify(alertRepository, never()).findById(anyString());
        verify(alertRepository, never()).save(any(Alert.class));
    }
    
    @Test
    void testAcknowledgeAlert_EmptyAlertId() {
        // Act & Assert
        assertThatThrownBy(() -> controller.acknowledgeAlert(""))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("alertId is required");
        
        verify(alertRepository, never()).findById(anyString());
        verify(alertRepository, never()).save(any(Alert.class));
    }
    
    @Test
    void testAcknowledgeAlert_AlertNotFound() {
        // Arrange
        String alertId = "non-existent-alert";
        when(alertRepository.findById(alertId)).thenReturn(null);
        
        // Act & Assert
        assertThatThrownBy(() -> controller.acknowledgeAlert(alertId))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Alert not found");
        
        verify(alertRepository, times(1)).findById(alertId);
        verify(alertRepository, never()).save(any(Alert.class));
    }
    
    @Test
    void testAcknowledgeAlert_AlreadyAcknowledged() {
        // Arrange
        String alertId = "alert-456";
        
        Alert mockAlert = new Alert();
        mockAlert.setId(alertId);
        mockAlert.setStatus(AlertStatus.ACKNOWLEDGED);
        mockAlert.setAcknowledgedAt(Instant.now().minusSeconds(3600));
        
        when(alertRepository.findById(alertId)).thenReturn(mockAlert);
        when(alertRepository.save(any(Alert.class))).thenAnswer(invocation -> {
            Alert alert = invocation.getArgument(0);
            return alert;
        });
        
        // Act
        Alert result = controller.acknowledgeAlert(alertId);
        
        // Assert - should update the acknowledgment time even if already acknowledged
        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isEqualTo(AlertStatus.ACKNOWLEDGED);
        assertThat(result.getAcknowledgedAt()).isNotNull();
        
        verify(alertRepository, times(1)).findById(alertId);
        verify(alertRepository, times(1)).save(any(Alert.class));
    }
    
    @Test
    void testAcknowledgeAlert_UpdatesTimestamp() {
        // Arrange
        String alertId = "alert-789";
        Instant beforeTest = Instant.now();
        
        Alert mockAlert = new Alert();
        mockAlert.setId(alertId);
        mockAlert.setStatus(AlertStatus.OPEN);
        
        when(alertRepository.findById(alertId)).thenReturn(mockAlert);
        when(alertRepository.save(any(Alert.class))).thenAnswer(invocation -> {
            Alert alert = invocation.getArgument(0);
            return alert;
        });
        
        // Act
        Alert result = controller.acknowledgeAlert(alertId);
        Instant afterTest = Instant.now();
        
        // Assert
        assertThat(result.getAcknowledgedAt()).isNotNull();
        assertThat(result.getAcknowledgedAt()).isBetween(beforeTest, afterTest);
        
        verify(alertRepository, times(1)).findById(alertId);
        verify(alertRepository, times(1)).save(any(Alert.class));
    }
    
    @Test
    void testAcknowledgeAlert_PreservesOtherFields() {
        // Arrange
        String alertId = "alert-999";
        
        Alert mockAlert = new Alert();
        mockAlert.setId(alertId);
        mockAlert.setStatus(AlertStatus.OPEN);
        mockAlert.setTitle("Important Alert");
        mockAlert.setDescription("This is a test alert");
        mockAlert.setSeverity(5);
        mockAlert.setRuleId("rule-123");
        
        when(alertRepository.findById(alertId)).thenReturn(mockAlert);
        when(alertRepository.save(any(Alert.class))).thenAnswer(invocation -> {
            Alert alert = invocation.getArgument(0);
            return alert;
        });
        
        // Act
        Alert result = controller.acknowledgeAlert(alertId);
        
        // Assert - all other fields should be preserved
        assertThat(result.getTitle()).isEqualTo("Important Alert");
        assertThat(result.getDescription()).isEqualTo("This is a test alert");
        assertThat(result.getSeverity()).isEqualTo(5);
        assertThat(result.getRuleId()).isEqualTo("rule-123");
        assertThat(result.getStatus()).isEqualTo(AlertStatus.ACKNOWLEDGED);
        
        verify(alertRepository, times(1)).findById(alertId);
        verify(alertRepository, times(1)).save(any(Alert.class));
    }
}
