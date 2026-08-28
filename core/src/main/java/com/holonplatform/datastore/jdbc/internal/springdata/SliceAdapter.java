/*
 * Copyright 2016-2024 Holon Platform (http://holon-platform.com/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.holonplatform.datastore.jdbc.internal.springdata;

import java.util.List;
import java.util.Objects;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;

import com.holonplatform.core.property.PropertyBox;
import com.holonplatform.core.query.Query;

/**
 * Spring Data {@link Slice} adapter for efficient lazy-loading pagination.
 * Avoids expensive COUNT queries by fetching one extra item to determine hasNext.
 *
 * @param <T> Result type
 */
public class SliceAdapter<T> {

    private final Query query;
    private final Class<?> resultType;

    public SliceAdapter(Query query, Class<?> resultType) {
        this.query = Objects.requireNonNull(query, "Query cannot be null");
        this.resultType = Objects.requireNonNull(resultType, "Result type cannot be null");
    }

    /**
     * Get a single page slice with lazy loading (no total count).
     * More efficient than Page for large result sets.
     * Query should already be sorted before wrapping with SliceAdapter.
     *
     * @param pageable Pagination parameters (sorting should be applied to query first)
     * @return Slice with hasNext() indicator
     */
    public Slice<PropertyBox> slice(Pageable pageable) {
        if (pageable == null) {
            pageable = Pageable.unpaged();
        }

        Query sliceQuery = query;

        // Request one extra item to detect if next page exists (lazy loading optimization)
        if (pageable.isPaged()) {
            int offset = (int) pageable.getOffset();
            int limit = pageable.getPageSize();
            sliceQuery = sliceQuery.limit(limit + 1).offset(offset);
        }

        List<PropertyBox> content = sliceQuery.list();
        boolean hasNext = false;

        if (pageable.isPaged() && content.size() > pageable.getPageSize()) {
            hasNext = true;
            // Trim to requested page size
            content = content.subList(0, pageable.getPageSize());
        }

        return new SliceImpl<>(content, pageable, hasNext);
    }
}
