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
package com.holonplatform.datastore.jdbc.internal.patterns;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for sealed type pattern matching.
 * Tests QueryCondition sealed interface, PatternMatcher, and FilterBuilder.
 *
 * @since 11.2
 */
@DisplayName("Pattern Matching Tests")
public class PatternMatchingTest {

    @Test
    @DisplayName("Should create QueryValue condition")
    void testQueryValue() {
        QueryCondition.QueryValue qv = new QueryCondition.QueryValue("test");
        
        assertEquals("VALUE", qv.getType());
        assertEquals("test", qv.value());
    }

    @Test
    @DisplayName("Should create QueryPredicate condition")
    void testQueryPredicate() {
        QueryCondition.QueryPredicate qp = new QueryCondition.QueryPredicate("name", "=", "John");
        
        assertEquals("PREDICATE", qp.getType());
        assertEquals("name", qp.property());
        assertEquals("=", qp.operator());
        assertEquals("John", qp.value());
    }

    @Test
    @DisplayName("Should create QueryComparison condition")
    void testQueryComparison() {
        QueryCondition.QueryComparison qc = new QueryCondition.QueryComparison(
            "age", 
            QueryCondition.ComparisonOp.GREATER_THAN, 
            18
        );
        
        assertEquals("COMPARISON", qc.getType());
        assertEquals("age", qc.property());
        assertEquals(QueryCondition.ComparisonOp.GREATER_THAN, qc.op());
        assertEquals(18, qc.value());
    }

    @Test
    @DisplayName("Should match QueryValue with pattern matching")
    void testMatchQueryValue() {
        QueryCondition condition = new QueryCondition.QueryValue("test");
        
        assertTrue(PatternMatcher.matches(condition, "test"));
        assertFalse(PatternMatcher.matches(condition, "other"));
    }

    @Test
    @DisplayName("Should match equals predicate")
    void testMatchEqualsPredicate() {
        QueryCondition condition = new QueryCondition.QueryPredicate("name", "=", "John");
        
        assertTrue(PatternMatcher.matches(condition, "John"));
        assertFalse(PatternMatcher.matches(condition, "Jane"));
    }

    @Test
    @DisplayName("Should match not equals predicate")
    void testMatchNotEqualsPredicate() {
        QueryCondition condition = new QueryCondition.QueryPredicate("status", "!=", "inactive");
        
        assertTrue(PatternMatcher.matches(condition, "active"));
        assertFalse(PatternMatcher.matches(condition, "inactive"));
    }

    @Test
    @DisplayName("Should match greater than comparison")
    void testMatchGreaterThan() {
        QueryCondition condition = new QueryCondition.QueryComparison(
            "age",
            QueryCondition.ComparisonOp.GREATER_THAN,
            18
        );
        
        assertTrue(PatternMatcher.matches(condition, 25));
        assertFalse(PatternMatcher.matches(condition, 15));
        assertFalse(PatternMatcher.matches(condition, 18));
    }

    @Test
    @DisplayName("Should match less than comparison")
    void testMatchLessThan() {
        QueryCondition condition = new QueryCondition.QueryComparison(
            "price",
            QueryCondition.ComparisonOp.LESS_THAN,
            100.0
        );
        
        assertTrue(PatternMatcher.matches(condition, 50.0));
        assertFalse(PatternMatcher.matches(condition, 150.0));
    }

    @Test
    @DisplayName("Should match IN comparison")
    void testMatchIn() {
        QueryCondition condition = new QueryCondition.QueryComparison(
            "status",
            QueryCondition.ComparisonOp.IN,
            List.of("active", "pending", "review")
        );
        
        assertTrue(PatternMatcher.matches(condition, "active"));
        assertTrue(PatternMatcher.matches(condition, "pending"));
        assertFalse(PatternMatcher.matches(condition, "inactive"));
    }

    @Test
    @DisplayName("Should match LIKE comparison")
    void testMatchLike() {
        QueryCondition condition = new QueryCondition.QueryComparison(
            "email",
            QueryCondition.ComparisonOp.LIKE,
            "%@example.com"
        );
        
        assertTrue(PatternMatcher.matches(condition, "user@example.com"));
        assertFalse(PatternMatcher.matches(condition, "user@other.com"));
    }

    @Test
    @DisplayName("Should match IS NULL comparison")
    void testMatchIsNull() {
        QueryCondition condition = new QueryCondition.QueryComparison(
            "deletedAt",
            QueryCondition.ComparisonOp.IS_NULL,
            null
        );
        
        assertTrue(PatternMatcher.matches(condition, null));
        assertFalse(PatternMatcher.matches(condition, "2024-01-01"));
    }

    @Test
    @DisplayName("Should match IS NOT NULL comparison")
    void testMatchIsNotNull() {
        QueryCondition condition = new QueryCondition.QueryComparison(
            "email",
            QueryCondition.ComparisonOp.IS_NOT_NULL,
            null
        );
        
        assertFalse(PatternMatcher.matches(condition, null));
        assertTrue(PatternMatcher.matches(condition, "user@example.com"));
    }

    @Test
    @DisplayName("Should validate conditions")
    void testValidateConditions() {
        QueryCondition validValue = new QueryCondition.QueryValue("value");
        QueryCondition validPredicate = new QueryCondition.QueryPredicate("prop", "=", "val");
        QueryCondition validComparison = new QueryCondition.QueryComparison("age", QueryCondition.ComparisonOp.EQUALS, 18);
        
        assertTrue(PatternMatcher.isValid(validValue));
        assertTrue(PatternMatcher.isValid(validPredicate));
        assertTrue(PatternMatcher.isValid(validComparison));
    }

    @Test
    @DisplayName("Should build filter with eq")
    void testFilterBuilderEq() {
        FilterBuilder builder = FilterBuilder.start()
            .eq("name", "John");
        
        assertEquals(1, builder.size());
        List<QueryCondition> conditions = builder.build();
        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0) instanceof QueryCondition.QueryComparison);
    }

    @Test
    @DisplayName("Should build complex filter with chaining")
    void testFilterBuilderChaining() {
        FilterBuilder builder = FilterBuilder.start()
            .eq("status", "active")
            .gt("age", 18)
            .lt("age", 65)
            .isNotNull("email");
        
        assertEquals(4, builder.size());
        assertFalse(builder.isEmpty());
    }

    @Test
    @DisplayName("Should build filter with IN condition")
    void testFilterBuilderIn() {
        FilterBuilder builder = FilterBuilder.start()
            .in("department", List.of("Engineering", "Sales", "Marketing"));
        
        assertEquals(1, builder.size());
        List<QueryCondition> conditions = builder.build();
        assertTrue(conditions.get(0) instanceof QueryCondition.QueryComparison);
    }

    @Test
    @DisplayName("Should build filter with LIKE condition")
    void testFilterBuilderLike() {
        FilterBuilder builder = FilterBuilder.start()
            .like("email", "%@example.com");
        
        assertEquals(1, builder.size());
        assertTrue(PatternMatcher.isValid(builder.build().get(0)));
    }

    @Test
    @DisplayName("Should clear filter builder")
    void testFilterBuilderClear() {
        FilterBuilder builder = FilterBuilder.start()
            .eq("status", "active")
            .ne("type", "delete");
        
        assertEquals(2, builder.size());
        builder.clear();
        assertEquals(0, builder.size());
        assertTrue(builder.isEmpty());
    }

    @Test
    @DisplayName("Should demonstrate sealed type pattern matching benefits")
    void testSealedTypePatternMatching() {
        List<QueryCondition> conditions = FilterBuilder.start()
            .eq("name", "John")
            .gt("age", 18)
            .isNotNull("email")
            .build();
        
        // Pattern matching ensures all cases are handled
        int totalConditions = 0;
        for (QueryCondition condition : conditions) {
            String type = switch (condition) {
                case QueryCondition.QueryValue qv -> "VALUE: " + qv.value();
                case QueryCondition.QueryPredicate qp -> "PREDICATE: " + qp.property() + " " + qp.operator();
                case QueryCondition.QueryComparison qc -> "COMPARISON: " + qc.property() + " " + qc.op();
            };
            totalConditions++;
            assertNotNull(type);
        }
        
        assertEquals(3, totalConditions);
    }

    @Test
    @DisplayName("Should handle comparison operators enum")
    void testComparisonOperatorsEnum() {
        QueryCondition.ComparisonOp eq = QueryCondition.ComparisonOp.EQUALS;
        QueryCondition.ComparisonOp gt = QueryCondition.ComparisonOp.GREATER_THAN;
        
        assertEquals("=", eq.getSymbol());
        assertEquals(">", gt.getSymbol());
        assertEquals("=", eq.toString());
    }

    @Test
    @DisplayName("Should perform complex filtering scenario")
    void testComplexFilteringScenario() {
        // Build a complex filter
        List<QueryCondition> filter = FilterBuilder.start()
            .eq("status", "active")
            .gte("joinDate", "2023-01-01")
            .lte("age", 65)
            .in("role", List.of("admin", "manager"))
            .like("email", "%@company.com")
            .isNotNull("phone")
            .build();
        
        assertEquals(6, filter.size());
        assertTrue(filter.stream().allMatch(PatternMatcher::isValid));
    }

    @Test
    @DisplayName("Should test type safety with sealed classes")
    void testTypeSafetyWithSealedClasses() {
        QueryCondition condition = new QueryCondition.QueryComparison("age", QueryCondition.ComparisonOp.GREATER_THAN, 18);
        
        // Compiler ensures only permitted types are possible
        assertNotNull(switch (condition) {
            case QueryCondition.QueryValue qv -> qv.value();
            case QueryCondition.QueryPredicate qp -> qp.property();
            case QueryCondition.QueryComparison qc -> qc.property();
        });
    }
}
