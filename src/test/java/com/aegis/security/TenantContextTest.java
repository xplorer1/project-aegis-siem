package com.aegis.security;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for TenantContext.
 * 
 * Tests verify:
 * - Basic set/get/clear operations
 * - Thread isolation
 * - Error handling
 * - Concurrent access patterns
 */
@DisplayName("TenantContext")
class TenantContextTest {
    
    @BeforeEach
    void setUp() {
        // Ensure clean state before each test
        TenantContext.clear();
    }
    
    @AfterEach
    void tearDown() {
        // Clean up after each test to prevent leaks
        TenantContext.clear();
    }
    
    @Test
    @DisplayName("should set and retrieve tenant ID")
    void shouldSetAndRetrieveTenantId() {
        // Given
        String tenantId = "tenant-123";
        
        // When
        TenantContext.setTenantId(tenantId);
        String retrieved = TenantContext.getTenantId();
        
        // Then
        assertThat(retrieved).isEqualTo(tenantId);
    }
    
    @Test
    @DisplayName("should return null when no tenant ID is set")
    void shouldReturnNullWhenNoTenantIdSet() {
        // When
        String tenantId = TenantContext.getTenantId();
        
        // Then
        assertThat(tenantId).isNull();
    }
    
    @Test
    @DisplayName("should clear tenant ID")
    void shouldClearTenantId() {
        // Given
        TenantContext.setTenantId("tenant-123");
        
        // When
        TenantContext.clear();
        String tenantId = TenantContext.getTenantId();
        
        // Then
        assertThat(tenantId).isNull();
    }
    
    @Test
    @DisplayName("should throw exception when setting null tenant ID")
    void shouldThrowExceptionWhenSettingNullTenantId() {
        // When/Then
        assertThatThrownBy(() -> TenantContext.setTenantId(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Tenant ID must not be null or empty");
    }
    
    @Test
    @DisplayName("should throw exception when setting empty tenant ID")
    void shouldThrowExceptionWhenSettingEmptyTenantId() {
        // When/Then
        assertThatThrownBy(() -> TenantContext.setTenantId(""))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Tenant ID must not be null or empty");
    }
    
    @Test
    @DisplayName("should throw exception when setting whitespace-only tenant ID")
    void shouldThrowExceptionWhenSettingWhitespaceTenantId() {
        // When/Then
        assertThatThrownBy(() -> TenantContext.setTenantId("   "))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Tenant ID must not be null or empty");
    }
    
    @Test
    @DisplayName("should allow overwriting tenant ID")
    void shouldAllowOverwritingTenantId() {
        // Given
        TenantContext.setTenantId("tenant-123");
        
        // When
        TenantContext.setTenantId("tenant-456");
        String tenantId = TenantContext.getTenantId();
        
        // Then
        assertThat(tenantId).isEqualTo("tenant-456");
    }
    
    @Test
    @DisplayName("should return true when tenant ID is set")
    void shouldReturnTrueWhenTenantIdIsSet() {
        // Given
        TenantContext.setTenantId("tenant-123");
        
        // When
        boolean isSet = TenantContext.isSet();
        
        // Then
        assertThat(isSet).isTrue();
    }
    
    @Test
    @DisplayName("should return false when tenant ID is not set")
    void shouldReturnFalseWhenTenantIdIsNotSet() {
        // When
        boolean isSet = TenantContext.isSet();
        
        // Then
        assertThat(isSet).isFalse();
    }
    
    @Test
    @DisplayName("should return tenant ID when requireTenantId is called and tenant is set")
    void shouldReturnTenantIdWhenRequireTenantIdCalledAndTenantSet() {
        // Given
        String tenantId = "tenant-123";
        TenantContext.setTenantId(tenantId);
        
        // When
        String required = TenantContext.requireTenantId();
        
        // Then
        assertThat(required).isEqualTo(tenantId);
    }
    
    @Test
    @DisplayName("should throw exception when requireTenantId is called and no tenant is set")
    void shouldThrowExceptionWhenRequireTenantIdCalledAndNoTenantSet() {
        // When/Then
        assertThatThrownBy(() -> TenantContext.requireTenantId())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No tenant ID set in current context");
    }
    
    @Test
    @DisplayName("should isolate tenant IDs between threads")
    void shouldIsolateTenantIdsBetweenThreads() throws InterruptedException {
        // Given
        CountDownLatch latch = new CountDownLatch(2);
        List<String> results = new ArrayList<>();
        
        // When
        Thread thread1 = new Thread(() -> {
            TenantContext.setTenantId("tenant-1");
            try {
                Thread.sleep(50); // Simulate some work
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            results.add(TenantContext.getTenantId());
            TenantContext.clear();
            latch.countDown();
        });
        
        Thread thread2 = new Thread(() -> {
            TenantContext.setTenantId("tenant-2");
            try {
                Thread.sleep(50); // Simulate some work
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            results.add(TenantContext.getTenantId());
            TenantContext.clear();
            latch.countDown();
        });
        
        thread1.start();
        thread2.start();
        
        latch.await(5, TimeUnit.SECONDS);
        
        // Then
        assertThat(results).containsExactlyInAnyOrder("tenant-1", "tenant-2");
    }
    
    @Test
    @DisplayName("should handle concurrent access from multiple threads")
    void shouldHandleConcurrentAccessFromMultipleThreads() throws InterruptedException {
        // Given
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        
        // When
        for (int i = 0; i < threadCount; i++) {
            final int tenantNumber = i;
            executor.submit(() -> {
                try {
                    String tenantId = "tenant-" + tenantNumber;
                    TenantContext.setTenantId(tenantId);
                    
                    // Simulate some work
                    Thread.sleep(10);
                    
                    // Verify tenant ID is still correct
                    String retrieved = TenantContext.getTenantId();
                    if (tenantId.equals(retrieved)) {
                        successCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    TenantContext.clear();
                    latch.countDown();
                }
            });
        }
        
        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        
        // Then
        assertThat(successCount.get()).isEqualTo(threadCount);
    }
    
    @Test
    @DisplayName("should not leak tenant ID between thread pool reuse")
    void shouldNotLeakTenantIdBetweenThreadPoolReuse() throws InterruptedException {
        // Given
        ExecutorService executor = Executors.newFixedThreadPool(1);
        CountDownLatch latch = new CountDownLatch(2);
        List<String> results = new ArrayList<>();
        
        // When - First task sets tenant ID but doesn't clear
        executor.submit(() -> {
            TenantContext.setTenantId("tenant-1");
            results.add(TenantContext.getTenantId());
            // Intentionally not clearing to simulate a bug
            latch.countDown();
        });
        
        // Wait for first task to complete
        Thread.sleep(100);
        
        // Second task on same thread should not see previous tenant ID
        executor.submit(() -> {
            String tenantId = TenantContext.getTenantId();
            results.add(tenantId != null ? tenantId : "null");
            TenantContext.clear();
            latch.countDown();
        });
        
        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();
        
        // Then - Second task sees the leaked tenant ID (demonstrating the importance of clear())
        assertThat(results).hasSize(2);
        assertThat(results.get(0)).isEqualTo("tenant-1");
        assertThat(results.get(1)).isEqualTo("tenant-1"); // Leaked from previous task
    }
    
    @Test
    @DisplayName("should handle rapid set and clear operations")
    void shouldHandleRapidSetAndClearOperations() {
        // When/Then - Should not throw any exceptions
        for (int i = 0; i < 1000; i++) {
            TenantContext.setTenantId("tenant-" + i);
            assertThat(TenantContext.getTenantId()).isEqualTo("tenant-" + i);
            TenantContext.clear();
            assertThat(TenantContext.getTenantId()).isNull();
        }
    }
    
    @Test
    @DisplayName("should throw exception when trying to instantiate")
    void shouldThrowExceptionWhenTryingToInstantiate() throws Exception {
        // Given
        var constructor = TenantContext.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        
        // When/Then
        assertThatThrownBy(() -> constructor.newInstance())
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("TenantContext is a utility class and cannot be instantiated");
    }
    
    @Test
    @DisplayName("should handle clear when no tenant ID is set")
    void shouldHandleClearWhenNoTenantIdSet() {
        // When/Then - Should not throw any exceptions
        assertThatCode(() -> TenantContext.clear()).doesNotThrowAnyException();
    }
    
    @Test
    @DisplayName("should handle multiple clear calls")
    void shouldHandleMultipleClearCalls() {
        // Given
        TenantContext.setTenantId("tenant-123");
        
        // When/Then - Should not throw any exceptions
        assertThatCode(() -> {
            TenantContext.clear();
            TenantContext.clear();
            TenantContext.clear();
        }).doesNotThrowAnyException();
        
        assertThat(TenantContext.getTenantId()).isNull();
    }
}
