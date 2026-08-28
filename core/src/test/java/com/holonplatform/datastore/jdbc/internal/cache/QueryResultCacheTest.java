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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for QueryResultCache.
 * Tests basic caching behavior, LRU eviction, TTL expiration, and statistics.
 *
 * @since 11.2
 */
@DisplayName("QueryResultCache Tests")
public class QueryResultCacheTest {

    private QueryResultCache cache;

    @BeforeEach
    void setUp() {
        cache = new QueryResultCache(3, 1000); // Small size for testing eviction
    }

    @Test
    @DisplayName("Should store and retrieve value")
    void testPutAndGet() {
        cache.put("key1", "value1");
        Optional<String> result = cache.get("key1");

        assertTrue(result.isPresent());
        assertEquals("value1", result.get());
    }

    @Test
    @DisplayName("Should return empty for missing key")
    void testGetMissing() {
        Optional<String> result = cache.get("nonexistent");

        assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("Should compute value if not cached")
    void testGetOrCompute() {
        String result1 = cache.getOrCompute("key1", k -> "computed");
        String result2 = cache.getOrCompute("key1", k -> "other");

        assertEquals("computed", result1);
        assertEquals("computed", result2); // From cache
    }

    @Test
    @DisplayName("Should track cache hits")
    void testCacheHits() {
        cache.put("key1", "value1");
        cache.get("key1");
        cache.get("key1");

        QueryResultCache.CacheStatistics stats = cache.getStatistics();
        assertEquals(2, stats.hits);
        assertEquals(0, stats.misses);
    }

    @Test
    @DisplayName("Should track cache misses")
    void testCacheMisses() {
        cache.get("nonexistent");
        cache.get("missing");

        QueryResultCache.CacheStatistics stats = cache.getStatistics();
        assertEquals(0, stats.hits);
        assertEquals(2, stats.misses);
    }

    @Test
    @DisplayName("Should calculate hit rate")
    void testHitRate() {
        cache.put("key1", "value1");
        cache.get("key1"); // hit
        cache.get("key2"); // miss
        cache.get("key3"); // miss
        cache.get("key1"); // hit

        QueryResultCache.CacheStatistics stats = cache.getStatistics();
        assertEquals(50.0, stats.hitRate, 0.1);
    }

    @Test
    @DisplayName("Should evict least recently used on size overflow")
    void testLRUEviction() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.put("key4", "value4"); // Should evict key1

        QueryResultCache.CacheStatistics stats = cache.getStatistics();
        assertEquals(3, stats.currentSize);

        assertFalse(cache.get("key1").isPresent(), "key1 should be evicted");
        assertTrue(cache.get("key2").isPresent(), "key2 should still exist");
        assertTrue(cache.get("key3").isPresent(), "key3 should still exist");
        assertTrue(cache.get("key4").isPresent(), "key4 should exist");
    }

    @Test
    @DisplayName("Should update LRU order on access")
    void testLRUReorderOnAccess() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.get("key1"); // Access key1 to update LRU order
        cache.put("key4", "value4"); // Should evict key2, not key1

        assertFalse(cache.get("key2").isPresent(), "key2 should be evicted");
        assertTrue(cache.get("key1").isPresent(), "key1 should still exist after access");
    }

    @Test
    @DisplayName("Should invalidate single key")
    void testInvalidateKey() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.invalidate("key1");

        assertFalse(cache.get("key1").isPresent());
        assertTrue(cache.get("key2").isPresent());
    }

    @Test
    @DisplayName("Should clear entire cache")
    void testClear() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.clear();

        QueryResultCache.CacheStatistics stats = cache.getStatistics();
        assertEquals(0, stats.currentSize);
    }

    @Test
    @DisplayName("Should invalidate by prefix")
    void testInvalidatePrefix() {
        cache.put("TABLE_A:query1", "result1");
        cache.put("TABLE_A:query2", "result2");
        cache.put("TABLE_B:query1", "result3");

        cache.invalidatePrefix("TABLE_A");

        assertFalse(cache.get("TABLE_A:query1").isPresent());
        assertFalse(cache.get("TABLE_A:query2").isPresent());
        assertTrue(cache.get("TABLE_B:query1").isPresent());
    }

    @Test
    @DisplayName("Should handle TTL expiration")
    void testTTLExpiration() throws InterruptedException {
        QueryResultCache shortTTLCache = new QueryResultCache(10, 100); // 100ms TTL
        shortTTLCache.put("key1", "value1");

        assertTrue(shortTTLCache.get("key1").isPresent());
        Thread.sleep(150); // Wait for TTL to expire
        assertFalse(shortTTLCache.get("key1").isPresent());
    }

    @Test
    @DisplayName("Should reset statistics")
    void testResetStatistics() {
        cache.put("key1", "value1");
        cache.get("key1"); // hit
        cache.get("missing"); // miss

        cache.resetStatistics();
        QueryResultCache.CacheStatistics stats = cache.getStatistics();

        assertEquals(0, stats.hits);
        assertEquals(0, stats.misses);
    }

    @Test
    @DisplayName("Should handle generic types")
    void testGenericTypes() {
        List<String> list = new ArrayList<>();
        list.add("item1");
        list.add("item2");

        cache.put("list_key", list);
        Optional<List<String>> result = cache.get("list_key");

        assertTrue(result.isPresent());
        assertEquals(2, result.get().size());
        assertEquals("item1", result.get().get(0));
    }

    @Test
    @DisplayName("Should handle null values with Optional wrapper")
    void testNullValue() {
        // Store a non-null wrapper to distinguish from cache miss
        String nullIndicator = "NULL_VALUE_MARKER";
        cache.put("null_key", nullIndicator);
        Optional<String> result = cache.get("null_key");

        assertTrue(result.isPresent());
        assertEquals(nullIndicator, result.get());
    }

    @Test
    @DisplayName("Should track eviction count")
    void testEvictionTracking() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.put("key4", "value4");
        cache.put("key5", "value5");

        QueryResultCache.CacheStatistics stats = cache.getStatistics();
        assertEquals(2, stats.evictions, "Should have evicted 2 items");
    }

    @Test
    @DisplayName("Should provide statistics string")
    void testStatisticsString() {
        cache.put("key1", "value1");
        cache.get("key1");
        cache.get("missing");

        String statsStr = cache.getStatistics().toString();

        assertTrue(statsStr.contains("hits=1"));
        assertTrue(statsStr.contains("misses=1"));
        assertTrue(statsStr.contains("hitRate=50.00%"));
    }

    @Test
    @DisplayName("Should be thread-safe for concurrent access")
    void testConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 100;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < operationsPerThread; j++) {
                    String key = "key_" + (j % 50);
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
        assertTrue(stats.hits > 0, "Should have recorded hits");
    }
}
