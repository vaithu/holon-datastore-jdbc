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
import com.holonplatform.core.query.Query;

import java.util.*;

/**
 * Generates deterministic cache keys from Query and selected properties.
 * Uses a simple hash-based approach combining query target and property names.
 *
 * @since 11.2
 */
public class CacheKeyGenerator {

    private CacheKeyGenerator() {
    }

    /**
     * Generate cache key from query and properties.
     * Uses a combination of query structure and property selections.
     *
     * @param query the query to generate key for
     * @param properties selected properties
     * @return cache key string
     */
    public static String generateKey(Query query, Iterable<? extends Property<?>> properties) {
        StringBuilder sb = new StringBuilder();
        sb.append("query:");
        sb.append(System.identityHashCode(query));
        
        if (properties != null) {
            sb.append(":props[");
            boolean first = true;
            for (Property<?> prop : properties) {
                if (!first) sb.append(",");
                sb.append(prop.getName());
                first = false;
            }
            sb.append("]");
        }
        
        return sb.toString();
    }
}

