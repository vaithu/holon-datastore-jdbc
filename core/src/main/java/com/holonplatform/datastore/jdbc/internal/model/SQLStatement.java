/*
 * Modern Java Records for JDBC Datastore
 * 
 * Provides immutable data transfer objects using Java 16+ records.
 */

package com.holonplatform.datastore.jdbc.internal.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Record for SQL statement representation.
 * Automatically generates constructor, equals, hashCode, toString.
 */
public record SQLStatement(String sql, List<Object> parameters) {
    
    public SQLStatement {
        Objects.requireNonNull(sql, "SQL cannot be null");
        if (sql.isBlank()) {
            throw new IllegalArgumentException("SQL cannot be blank");
        }
        if (parameters == null) {
            parameters = Collections.emptyList();
        }
    }
    
    public boolean hasParameters() {
        return !parameters.isEmpty();
    }
}
