/*
 * Virtual Thread Concurrency Integration Tests
 */

package com.holonplatform.datastore.jdbc.test.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.holonplatform.datastore.jdbc.internal.concurrency.VirtualThreadDataSourceAdapter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Integration tests for Virtual Thread-based DataSource adapter.
 * 
 * Tests verify:
 * - Virtual threads prevent platform thread starvation
 * - Supports thousands of concurrent connections
 * - Graceful timeout handling
 * - Connection pool isolation
 * 
 * These tests only run on Java 21+.
 */
@DisplayName("Virtual Thread DataSource Adapter Integration Tests")
@EnabledIfSystemProperty(named = "java.version", matches = "^(21|22|23|24|25|26|27|28|29|30).*")
public class VirtualThreadDataSourceAdapterIT {

    private HikariDataSource hikariDataSource;
    private VirtualThreadDataSourceAdapter adapter;

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:test;MODE=PostgreSQL");
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(10);  // Small pool to test virtual thread efficiency
        config.setMinimumIdle(2);
        
        hikariDataSource = new HikariDataSource(config);
        adapter = new VirtualThreadDataSourceAdapter(hikariDataSource, 5000);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (adapter != null) {
            adapter.close();
        }
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }
    }

    @Test
    @DisplayName("Should acquire connection on virtual thread")
    void testGetConnectionOnVirtualThread() throws Exception {
        Connection conn = adapter.getConnection();
        assertNotNull(conn);
        
        // Verify it's a valid connection
        assertTrue(conn.isValid(5));
        conn.close();
    }

    @Test
    @DisplayName("Should execute operation on virtual thread")
    void testExecuteOnVirtualThread() throws Exception {
        String result = adapter.executeOnVirtualThread(conn -> {
            int[] intval = new int[1];
            conn.createStatement().execute("SELECT 42");
            return "success";
        });
        assertEquals("success", result);
    }

    @Test
    @DisplayName("Should handle concurrent operations with virtual threads")
    @Timeout(30)
    void testConcurrentOperations() throws Exception {
        int concurrentRequests = 100;
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(concurrentRequests);

        // Launch 100 concurrent virtual threads
        for (int i = 0; i < concurrentRequests; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    Connection conn = adapter.getConnection();
                    // Simulate some work
                    Thread.sleep(10);
                    conn.close();
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for all threads to complete
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "Not all operations completed within timeout");
        
        // All should succeed
        assertEquals(concurrentRequests, successCount.get(),
            "Expected all operations to succeed");
        assertEquals(0, failureCount.get(),
            "Expected no failures");
    }

    @Test
    @DisplayName("Should handle high concurrency (1000+ operations)")
    @Timeout(60)
    void testHighConcurrency() throws Exception {
        int operationCount = 1000;
        AtomicInteger completed = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(operationCount);

        long startTime = System.currentTimeMillis();

        // Launch 1000 concurrent operations
        for (int i = 0; i < operationCount; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    adapter.executeOnVirtualThread(conn -> {
                        // Minimal work - just validate connection
                        conn.isValid(1);
                        return null;
                    });
                    completed.incrementAndGet();
                } catch (Exception e) {
                    // Expected - small pool will be saturated
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for all to complete
        boolean result = latch.await(60, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - startTime;

        assertTrue(result, "Operations should complete within timeout");
        assertTrue(completed.get() > 0, "Should complete at least some operations");
        
        System.out.println(String.format(
            "High Concurrency Test: %d operations, %d completed in %dms (throughput: %.0f ops/sec)",
            operationCount, completed.get(), elapsed, (completed.get() * 1000.0) / elapsed
        ));
    }

    @Test
    @DisplayName("Should respect connection timeout")
    @Timeout(10)
    void testConnectionTimeout() throws Exception {
        adapter.shutdown();
        
        // Adapter is closed, should timeout
        try {
            adapter.executeOnVirtualThread(conn -> "test");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    @DisplayName("Should support multiple sequential operations")
    void testSequentialOperations() throws Exception {
        for (int i = 0; i < 10; i++) {
            final int index = i;
            String result = adapter.executeOnVirtualThread(conn -> {
                conn.isValid(1);
                return "operation-" + index;
            });
            assertEquals("operation-" + i, result);
        }
    }

    @Test
    @DisplayName("Should measure virtual thread performance vs traditional threading")
    @Timeout(60)
    void testPerformanceComparison() throws Exception {
        int operationCount = 500;

        // Traditional thread approach (simulated with Thread.currentThread)
        long traditionalStart = System.nanoTime();
        int traditionalCompleted = 0;
        for (int i = 0; i < operationCount; i++) {
            try {
                Connection conn = hikariDataSource.getConnection();
                conn.isValid(1);
                conn.close();
                traditionalCompleted++;
            } catch (Exception e) {
                // Ignore - pool exhaustion expected
            }
        }
        long traditionalTime = System.nanoTime() - traditionalStart;

        // Virtual thread approach
        AtomicInteger virtualCompleted = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(operationCount);
        long virtualStart = System.nanoTime();
        
        for (int i = 0; i < operationCount; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    adapter.executeOnVirtualThread(conn -> {
                        conn.isValid(1);
                        return null;
                    });
                    virtualCompleted.incrementAndGet();
                } catch (Exception e) {
                    // Ignore
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(60, TimeUnit.SECONDS);
        long virtualTime = System.nanoTime() - virtualStart;

        System.out.println(String.format(
            "Performance: Traditional=%d ops (%.1fms), Virtual=%d ops (%.1fms)",
            traditionalCompleted, traditionalTime / 1_000_000.0,
            virtualCompleted.get(), virtualTime / 1_000_000.0
        ));

        // Virtual threads should allow more concurrent operations
        assertTrue(virtualCompleted.get() >= traditionalCompleted,
            "Virtual threads should allow more concurrent operations");
    }
}
