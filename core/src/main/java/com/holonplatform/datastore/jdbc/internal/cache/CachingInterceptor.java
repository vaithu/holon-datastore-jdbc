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

/**
 * Cache invalidator for mutation operations.
 * Can be used to clear cache when data is modified.
 *
 * @since 11.2
 */
public class CachingInterceptor {

    private final QueryResultCache cache;
    private final Set<String> invalidationTargets = Collections.synchronizedSet(new HashSet<>());

    public CachingInterceptor(QueryResultCache cache) {
        this.cache = Objects.requireNonNull(cache, "Cache must not be null");
    }

    /**
     * Clear entire cache (call on any mutation).
     */
    public void invalidateAll() {
        cache.clear();
    }

    /**
     * Invalidate cache entries by prefix.
     *
     * @param keyPrefix prefix to match
     */
    public void invalidatePrefix(String keyPrefix) {
        cache.invalidatePrefix(keyPrefix);
        invalidationTargets.add(keyPrefix);
    }

    /**
     * Get targets that have been invalidated since last reset.
     *
     * @return set of invalidated target prefixes
     */
    public Set<String> getInvalidatedTargets() {
        return new HashSet<>(invalidationTargets);
    }

    /**
     * Reset invalidation tracking.
     */
    public void resetTracking() {
        invalidationTargets.clear();
    }
}

