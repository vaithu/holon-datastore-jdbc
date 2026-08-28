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

import com.holonplatform.core.property.Property;
import com.holonplatform.core.property.PropertyBox;
import com.holonplatform.core.query.Query;

import java.util.*;
import java.util.stream.Stream;

/**
 * Query wrapper that caches query results.
 * Transparently intercepts list(), findOne(), stream(), and count() operations.
 *
 * @since 11.2
 */
public class CacheableQuery {

    private final Query delegate;
    private final QueryResultCache cache;

    public CacheableQuery(Query delegate, QueryResultCache cache) {
        this.delegate = Objects.requireNonNull(delegate, "Query must not be null");
        this.cache = Objects.requireNonNull(cache, "Cache must not be null");
    }

    /**
     * Execute list query with caching.
     *
     * @param properties properties to retrieve
     * @return list of PropertyBox results
     */
    @SuppressWarnings("unchecked")
    public List<PropertyBox> list(Iterable<? extends Property<?>> properties) {
        String cacheKey = CacheKeyGenerator.generateKey(delegate, (Iterable<? extends Property<?>>) properties);
        return cache.getOrCompute(cacheKey, key -> delegate.list(properties));
    }

    /**
     * Execute find one query with caching.
     *
     * @param properties properties to retrieve
     * @return Optional PropertyBox result
     */
    @SuppressWarnings("unchecked")
    public Optional<PropertyBox> findOne(Iterable<? extends Property<?>> properties) {
        String cacheKey = CacheKeyGenerator.generateKey(delegate, (Iterable<? extends Property<?>>) properties);
        return cache.getOrCompute(cacheKey, key -> delegate.findOne(properties));
    }

    /**
     * Execute stream query with caching (materializes to list then streams).
     *
     * @param properties properties to retrieve
     * @return Stream of PropertyBox results
     */
    @SuppressWarnings("unchecked")
    public Stream<PropertyBox> stream(Iterable<? extends Property<?>> properties) {
        String cacheKey = CacheKeyGenerator.generateKey(delegate, (Iterable<? extends Property<?>>) properties);
        List<PropertyBox> results = cache.getOrCompute(cacheKey, key -> delegate.list(properties));
        return results.stream();
    }

    /**
     * Execute count query with caching.
     *
     * @return count result
     */
    public long count() {
        String cacheKey = "count:" + System.identityHashCode(delegate);
        return cache.getOrCompute(cacheKey, key -> delegate.count());
    }

    /**
     * Invalidate cache entry for this query.
     */
    public void invalidateCache() {
        cache.clear();
    }

    /**
     * Get the underlying delegate query.
     *
     * @return delegate Query
     */
    public Query getDelegate() {
        return delegate;
    }

    /**
     * Get the cache statistics.
     *
     * @return cache statistics
     */
    public QueryResultCache.CacheStatistics getCacheStatistics() {
        return cache.getStatistics();
    }
}
