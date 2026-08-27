/*
 * Query Result Record
 */

package com.holonplatform.datastore.jdbc.internal.model;

import java.util.List;

/**
 * Record for paginated query results.
 * Type-safe representation of result pages.
 */
public record QueryResultPage<T>(
    List<T> items,
    long total,
    int pageNumber,
    int pageSize
) {
    
    public boolean hasMore() {
        return (long)(pageNumber + 1) * pageSize < total;
    }
    
    public int getTotalPages() {
        return (int) Math.ceil((double) total / pageSize);
    }
}
