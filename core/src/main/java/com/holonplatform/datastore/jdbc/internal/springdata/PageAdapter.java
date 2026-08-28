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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import com.holonplatform.core.property.PropertyBox;
import com.holonplatform.core.query.Query;

/**
 * Spring Data {@link Page} adapter for Holon Query results.
 * Provides lazy pagination with OFFSET/LIMIT optimization.
 * Note: Sorting is handled by the caller via query modification before wrapping.
 *
 * @param <T> Result type
 */
public class PageAdapter<T> {

    private final Query query;
    private final Class<?> resultType;

    public PageAdapter(Query query, Class<?> resultType) {
        this.query = Objects.requireNonNull(query, "Query cannot be null");
        this.resultType = Objects.requireNonNull(resultType, "Result type cannot be null");
    }

    /**
     * Paginate query results using Spring Data Pageable.
     * Query should already be sorted before wrapping with PageAdapter.
     *
     * @param pageable Pagination parameters (sorting should be applied to query first)
     * @return Page containing results for requested page
     */
    public Page<PropertyBox> page(Pageable pageable) {
        if (pageable == null) {
            pageable = Pageable.unpaged();
        }

        // Count total results (needed for Page metadata)
        long total = query.count();

        if (total == 0) {
            return new PageImpl<>(new ArrayList<>(), pageable, 0);
        }

        Query pageQuery = query;

        // Apply pagination with OFFSET/LIMIT (most efficient for databases)
        if (pageable.isPaged()) {
            int offset = (int) pageable.getOffset();
            int limit = pageable.getPageSize();
            pageQuery = pageQuery.limit(limit).offset(offset);
        }

        // Execute query and materialize results
        List<PropertyBox> content = pageQuery.list();

        return new PageImpl<>(content, pageable, total);
    }

    /**
     * Get a single page slice without total count (more efficient).
     *
     * @param pageable Pagination parameters
     * @return Slice with hasNext() capability
     */
    public org.springframework.data.domain.Slice<PropertyBox> slice(Pageable pageable) {
        if (pageable == null) {
            pageable = Pageable.unpaged();
        }

        Query sliceQuery = query;

        // Request one extra item to detect if next page exists
        if (pageable.isPaged()) {
            int offset = (int) pageable.getOffset();
            int limit = pageable.getPageSize();
            sliceQuery = sliceQuery.limit(limit + 1).offset(offset);
        }

        List<PropertyBox> content = sliceQuery.list();
        boolean hasNext = false;

        if (pageable.isPaged() && content.size() > pageable.getPageSize()) {
            hasNext = true;
            content = content.subList(0, pageable.getPageSize());
        }

        return new org.springframework.data.domain.SliceImpl<>(content, pageable, hasNext);
    }
}
