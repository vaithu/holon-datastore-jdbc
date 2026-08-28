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

import java.util.Collection;
import java.util.Objects;

/**
 * Pattern matcher utility for evaluating query conditions.
 * Uses sealed type pattern matching to safely match and evaluate conditions.
 *
 * @since 11.2
 */
public class PatternMatcher {

    private PatternMatcher() {
    }

    /**
     * Evaluate a condition against a value using sealed pattern matching.
     *
     * @param condition the query condition to evaluate
     * @param actualValue the actual value to compare
     * @return true if condition matches the actual value
     */
    public static boolean matches(QueryCondition condition, Object actualValue) {
        Objects.requireNonNull(condition, "Condition must not be null");

        return switch (condition) {
            case QueryCondition.QueryValue qv -> 
                actualValue != null && actualValue.equals(qv.value());
            
            case QueryCondition.QueryPredicate qp -> 
                matchPredicate(qp, actualValue);
            
            case QueryCondition.QueryComparison qc -> 
                matchComparison(qc, actualValue);
        };
    }

    /**
     * Evaluate a predicate condition.
     *
     * @param predicate the predicate to evaluate
     * @param value the value to test
     * @return true if predicate matches
     */
    private static boolean matchPredicate(QueryCondition.QueryPredicate predicate, Object value) {
        return switch (predicate.operator()) {
            case "=" -> value != null && value.equals(predicate.value());
            case "!=" -> value == null || !value.equals(predicate.value());
            case ">" -> compareNumeric(value, predicate.value()) > 0;
            case "<" -> compareNumeric(value, predicate.value()) < 0;
            case ">=" -> compareNumeric(value, predicate.value()) >= 0;
            case "<=" -> compareNumeric(value, predicate.value()) <= 0;
            case "IN" -> isInCollection(value, predicate.value());
            case "NOT IN" -> !isInCollection(value, predicate.value());
            case "LIKE" -> matchLike(value, predicate.value());
            default -> false;
        };
    }

    /**
     * Evaluate a comparison condition.
     *
     * @param comparison the comparison to evaluate
     * @param value the value to test
     * @return true if comparison matches
     */
    private static boolean matchComparison(QueryCondition.QueryComparison comparison, Object value) {
        return switch (comparison.op()) {
            case EQUALS -> value != null && value.equals(comparison.value());
            case NOT_EQUALS -> value == null || !value.equals(comparison.value());
            case GREATER_THAN -> compareNumeric(value, comparison.value()) > 0;
            case LESS_THAN -> compareNumeric(value, comparison.value()) < 0;
            case GREATER_THAN_OR_EQUAL -> compareNumeric(value, comparison.value()) >= 0;
            case LESS_THAN_OR_EQUAL -> compareNumeric(value, comparison.value()) <= 0;
            case IN -> isInCollection(value, comparison.value());
            case NOT_IN -> !isInCollection(value, comparison.value());
            case LIKE -> matchLike(value, comparison.value());
            case IS_NULL -> value == null;
            case IS_NOT_NULL -> value != null;
        };
    }

    /**
     * Compare two numeric values.
     *
     * @param actual the actual value
     * @param expected the expected value
     * @return -1 if actual < expected, 0 if equal, 1 if actual > expected
     */
    @SuppressWarnings("unchecked")
    private static int compareNumeric(Object actual, Object expected) {
        if (actual == null) return -1;
        if (expected == null) return 1;

        if (actual instanceof Comparable && expected instanceof Comparable) {
            try {
                return ((Comparable<Object>) actual).compareTo(expected);
            } catch (ClassCastException e) {
                return 0;
            }
        }

        return 0;
    }

    /**
     * Check if value is in collection.
     *
     * @param value the value to check
     * @param collection the collection (should be Collection<?>)
     * @return true if value is in collection
     */
    private static boolean isInCollection(Object value, Object collection) {
        if (value == null) return false;
        if (collection instanceof Collection<?>) {
            return ((Collection<?>) collection).contains(value);
        }
        return false;
    }

    /**
     * Match value against LIKE pattern (% wildcards).
     *
     * @param actual the actual value
     * @param pattern the pattern (with % wildcards)
     * @return true if matches pattern
     */
    private static boolean matchLike(Object actual, Object pattern) {
        if (actual == null || pattern == null) return false;
        if (!(pattern instanceof String)) return false;

        String str = actual.toString();
        String pat = (String) pattern;
        
        // Simple LIKE implementation: convert % to regex .*
        String regex = pat.replace("%", ".*").replace("_", ".");
        return str.matches(regex);
    }

    /**
     * Check if a condition is satisfiable (basic validation).
     *
     * @param condition the condition to check
     * @return true if condition appears valid
     */
    public static boolean isValid(QueryCondition condition) {
        Objects.requireNonNull(condition, "Condition must not be null");

        return switch (condition) {
            case QueryCondition.QueryValue qv -> qv.value() != null;
            case QueryCondition.QueryPredicate qp -> 
                qp.property() != null && !qp.property().isEmpty() && 
                qp.operator() != null && !qp.operator().isEmpty();
            case QueryCondition.QueryComparison qc -> 
                qc.property() != null && !qc.property().isEmpty();
        };
    }
}
