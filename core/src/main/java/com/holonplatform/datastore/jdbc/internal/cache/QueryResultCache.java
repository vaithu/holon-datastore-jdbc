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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Thread-safe in-memory query result cache with LRU eviction and TTL support.
 * Provides metrics tracking for hit rate, miss rate, and eviction statistics.
 *
 * @since 11.2
 */
public class QueryResultCache {

    private static final long DEFAULT_TTL_MILLIS = 5 * 60 * 1000; // 5 minutes
    private static final int DEFAULT_MAX_SIZE = 1000;

    private final int maxSize;
    private final long ttlMillis;
    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final LinkedHashMap<String, Long> lruOrder = new LinkedHashMap<>(16, 0.75f, true);
    private final Object lruLock = new Object();

    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);

    public QueryResultCache() {
        this(DEFAULT_MAX_SIZE, DEFAULT_TTL_MILLIS);
    }

    public QueryResultCache(int maxSize, long ttlMillis) {
        this.maxSize = maxSize > 0 ? maxSize : DEFAULT_MAX_SIZE;
        this.ttlMillis = ttlMillis > 0 ? ttlMillis : DEFAULT_TTL_MILLIS;
    }

    /**
     * Get cached value or compute using the provider function.
     *
     * @param key cache key
     * @param provider function to compute value if not cached
     * @return cached or computed value
     */
    @SuppressWarnings("unchecked")
    public <T> T getOrCompute(String key, Function<String, T> provider) {
        CacheEntry entry = cache.get(key);

        if (entry != null && !isExpired(entry)) {
            hits.incrementAndGet();
            updateLRU(key);
            return (T) entry.value;
        }

        misses.incrementAndGet();
        T value = provider.apply(key);
        put(key, value);
        return value;
    }

    /**
     * Put value in cache.
     *
     * @param key cache key
     * @param value value to cache
     */
    public void put(String key, Object value) {
        long now = System.currentTimeMillis();
        cache.put(key, new CacheEntry(value, now));
        updateLRU(key);
    }

    /**
     * Get cached value if exists and not expired.
     *
     * @param key cache key
     * @return cached value or empty Optional
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> get(String key) {
        CacheEntry entry = cache.get(key);

        if (entry != null && !isExpired(entry)) {
            hits.incrementAndGet();
            updateLRU(key);
            return Optional.of((T) entry.value);
        }

        if (entry != null) {
            cache.remove(key);
            removeFromLRU(key);
        }

        misses.incrementAndGet();
        return Optional.empty();
    }

    /**
     * Remove entry from cache.
     *
     * @param key cache key
     */
    public void invalidate(String key) {
        cache.remove(key);
        removeFromLRU(key);
    }

    /**
     * Clear entire cache.
     */
    public void clear() {
        cache.clear();
        synchronized (lruLock) {
            lruOrder.clear();
        }
    }

    /**
     * Invalidate entries matching prefix (for table/entity invalidation).
     *
     * @param keyPrefix prefix to match (e.g., "MY_TABLE" to invalidate all MY_TABLE queries)
     */
    public void invalidatePrefix(String keyPrefix) {
        cache.keySet().stream()
                .filter(k -> k.startsWith(keyPrefix))
                .forEach(this::invalidate);
    }

    /**
     * Get cache statistics.
     *
     * @return statistics snapshot
     */
    public CacheStatistics getStatistics() {
        long hitCount = hits.get();
        long missCount = misses.get();
        long total = hitCount + missCount;
        double hitRate = total > 0 ? (double) hitCount / total * 100 : 0;

        return new CacheStatistics(
                hitCount,
                missCount,
                hitRate,
                evictions.get(),
                cache.size(),
                maxSize
        );
    }

    /**
     * Reset statistics counters.
     */
    public void resetStatistics() {
        hits.set(0);
        misses.set(0);
        evictions.set(0);
    }

    private boolean isExpired(CacheEntry entry) {
        return System.currentTimeMillis() - entry.createdAt > ttlMillis;
    }

    private void updateLRU(String key) {
        synchronized (lruLock) {
            // Remove if exists to re-add at end (most recent)
            lruOrder.remove(key);
            lruOrder.put(key, System.currentTimeMillis());
            
            // Check if we need to evict
            if (lruOrder.size() > maxSize) {
                // Get the first (least recently used) key
                String eldestKey = lruOrder.keySet().iterator().next();
                lruOrder.remove(eldestKey);
                cache.remove(eldestKey);
                evictions.incrementAndGet();
            }
        }
    }

    private void removeFromLRU(String key) {
        synchronized (lruLock) {
            lruOrder.remove(key);
        }
    }

    /**
     * Cache entry holding value and creation timestamp.
     */
    private static class CacheEntry {
        final Object value;
        final long createdAt;

        CacheEntry(Object value, long createdAt) {
            this.value = value;
            this.createdAt = createdAt;
        }
    }

    /**
     * Cache statistics snapshot.
     */
    public static class CacheStatistics {
        public final long hits;
        public final long misses;
        public final double hitRate;
        public final long evictions;
        public final int currentSize;
        public final int maxSize;

        public CacheStatistics(long hits, long misses, double hitRate, long evictions, int currentSize, int maxSize) {
            this.hits = hits;
            this.misses = misses;
            this.hitRate = hitRate;
            this.evictions = evictions;
            this.currentSize = currentSize;
            this.maxSize = maxSize;
        }

        @Override
        public String toString() {
            return String.format(
                    "CacheStatistics{hits=%d, misses=%d, hitRate=%.2f%%, evictions=%d, size=%d/%d}",
                    hits, misses, hitRate, evictions, currentSize, maxSize
            );
        }
    }
}
