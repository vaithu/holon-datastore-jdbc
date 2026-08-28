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

import java.util.Objects;

/**
 * Sealed interface for type-safe query conditions.
 * Represents different types of query value patterns and predicates.
 *
 * Permitted implementations:
 * - QueryValue: Represents a constant value in a query
 * - QueryPredicate: Represents a predicate condition
 * - QueryComparison: Represents a comparison operation
 *
 * @since 11.2
 */
public sealed interface QueryCondition permits QueryCondition.QueryValue, QueryCondition.QueryPredicate, QueryCondition.QueryComparison {

    /**
     * Get the condition type.
     *
     * @return condition type
     */
    String getType();

    /**
     * Constant value in a query.
     * Example: WHERE name = 'John'
     */
    record QueryValue(Object value) implements QueryCondition {
        @Override
        public String getType() {
            return "VALUE";
        }

        @Override
        public String toString() {
            return String.format("QueryValue(%s)", value);
        }
    }

    /**
     * Query predicate condition.
     * Example: WHERE active = true
     */
    record QueryPredicate(String property, String operator, Object value) implements QueryCondition {
        public QueryPredicate {
            Objects.requireNonNull(property, "Property must not be null");
            Objects.requireNonNull(operator, "Operator must not be null");
        }

        @Override
        public String getType() {
            return "PREDICATE";
        }

        @Override
        public String toString() {
            return String.format("QueryPredicate(%s %s %s)", property, operator, value);
        }
    }

    /**
     * Query comparison operation.
     * Example: WHERE age > 18 AND age < 65
     */
    record QueryComparison(String property, ComparisonOp op, Object value) implements QueryCondition {
        public QueryComparison {
            Objects.requireNonNull(property, "Property must not be null");
            Objects.requireNonNull(op, "Comparison operator must not be null");
        }

        @Override
        public String getType() {
            return "COMPARISON";
        }

        @Override
        public String toString() {
            return String.format("QueryComparison(%s %s %s)", property, op, value);
        }
    }

    /**
     * Comparison operators for type-safe filtering.
     */
    enum ComparisonOp {
        EQUALS("="),
        NOT_EQUALS("!="),
        GREATER_THAN(">"),
        LESS_THAN("<"),
        GREATER_THAN_OR_EQUAL(">="),
        LESS_THAN_OR_EQUAL("<="),
        IN("IN"),
        NOT_IN("NOT IN"),
        LIKE("LIKE"),
        IS_NULL("IS NULL"),
        IS_NOT_NULL("IS NOT NULL");

        private final String symbol;

        ComparisonOp(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return symbol;
        }

        @Override
        public String toString() {
            return symbol;
        }
    }
}
