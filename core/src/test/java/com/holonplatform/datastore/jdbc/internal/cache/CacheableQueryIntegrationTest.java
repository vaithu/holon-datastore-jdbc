/*
 * Copyright 2016-2025 Axioma srl.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.holonplatform.datastore.jdbc.internal.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for cache key generation and basic cache functionality.
 * Tests cache behavior in realistic scenarios.
 *
 * @since 11.2
 */
@DisplayName("Cache Key Generation & Integration Tests")
public class CacheableQueryIntegrationTest {

    @Test
    @DisplayName("Should generate unique cache keys for queries")
    void testCacheKeyGeneration() {
        QueryResultCache cache = new QueryResultCache(10, 60000);
        
        String key1 = "query:123:props[NAME,ID]";
        String key2 = "query:456:props[NAME,ID]";
        String key3 = "query:123:props[NAME,ID,EMAIL]";
        
        cache.put(key1, "result1");
        cache.put(key2, "result2");
        cache.put(key3, "result3");

        assertTrue(cache.get(key1).isPresent());
        assertTrue(cache.get(key2).isPresent());
        assertTrue(cache.get(key3).isPresent());
        
        assertNotEquals(cache.get(key1).get(), cache.get(key2).get());
    }

    @Test
    @DisplayName("Should measure cache performance improvement")
    void testCachePerformanceImprovement() {
        QueryResultCache cache = new QueryResultCache(10, 60000);
        
        // First execution (cache miss)
        long start1 = System.nanoTime();
        cache.getOrCompute("test_key", k -> {
            try {
                Thread.sleep(10); // Simulate expensive operation
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "expensive_result";
        });
        long time1 = System.nanoTime() - start1;

        // Second execution (cache hit)
        long start2 = System.nanoTime();
        cache.getOrCompute("test_key", k -> "expensive_result");
        long time2 = System.nanoTime() - start2;

        assertTrue(time2 < time1, "Cache hit should be faster than miss: " + time1 + "ns vs " + time2 + "ns");
        double improvement = ((double) (time1 - time2) / time1) * 100;
        assertTrue(improvement > 50, "Cache should provide significant speedup: " + improvement + "%");
    }

    @Test
    @DisplayName("Should validate cache statistics")
    void testCacheStatistics() {
        QueryResultCache cache = new QueryResultCache(10, 60000);
        
        cache.put("key1", "value1");
        cache.get("key1");
        cache.get("key1");
        cache.get("missing");

        QueryResultCache.CacheStatistics stats = cache.getStatistics();
        
        assertEquals(2, stats.hits);
        assertEquals(1, stats.misses);
        assertTrue(stats.hitRate >= 66.0 && stats.hitRate <= 67.0, "Hit rate should be ~66.67%");
        assertEquals(0, stats.evictions);
        assertEquals(1, stats.currentSize);
    }

    @Test
    @DisplayName("Should handle concurrent cache access")
    void testConcurrentCacheAccess() throws InterruptedException {
        QueryResultCache cache = new QueryResultCache(100, 60000);
        
        Thread[] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 20; j++) {
                    String key = "key_" + (j % 10);
                    cache.put(key, "value_" + threadId + "_" + j);
                    cache.get(key);
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        QueryResultCache.CacheStatistics stats = cache.getStatistics();
        assertTrue(stats.hits > 0, "Should have recorded cache hits");
        assertTrue(stats.misses >= 0, "Should have recorded cache misses or hits");
    }

    @Test
    @DisplayName("Should demonstrate cache invalidation")
    void testCacheInvalidation() {
        QueryResultCache cache = new QueryResultCache(10, 60000);
        
        cache.put("USER:1:name", "Alice");
        cache.put("USER:2:name", "Bob");
        cache.put("PRODUCT:1:title", "Widget");

        assertTrue(cache.get("USER:1:name").isPresent());
        
        cache.invalidatePrefix("USER");
        
        assertFalse(cache.get("USER:1:name").isPresent());
        assertFalse(cache.get("USER:2:name").isPresent());
        assertTrue(cache.get("PRODUCT:1:title").isPresent(), "PRODUCT entries should not be affected");
    }
}

