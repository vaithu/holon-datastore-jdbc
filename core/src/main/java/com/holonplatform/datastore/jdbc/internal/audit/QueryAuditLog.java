/*
 * Copyright 2016-2017 Axioma srl.
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
package com.holonplatform.datastore.jdbc.internal.audit;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable record capturing details of a SQL query execution for auditing and monitoring.
 * 
 * This record provides a convenient way to store query execution metrics including:
 * - SQL text and parameters
 * - Execution time and timestamp
 * - Row counts (affected/returned)
 * - Success/failure status
 * 
 * @since 11.1.0
 */
public record QueryAuditLog(
		String sql,
		List<Object> parameters,
		long executionTimeMs,
		Instant executedAt,
		int rowsAffected,
		int rowsReturned,
		boolean successful,
		String errorMessage,
		String operationType
) {

	/**
	 * Constructor with validation.
	 */
	public QueryAuditLog {
		Objects.requireNonNull(sql, "sql cannot be null");
		Objects.requireNonNull(executedAt, "executedAt cannot be null");
		Objects.requireNonNull(operationType, "operationType cannot be null");
		
		// Make parameters unmodifiable
		if (parameters != null) {
			parameters = Collections.unmodifiableList(parameters);
		}
	}

	/**
	 * Builder for creating QueryAuditLog instances.
	 */
	public static class Builder {
		private String sql;
		private List<Object> parameters;
		private long executionTimeMs;
		private Instant executedAt;
		private int rowsAffected;
		private int rowsReturned;
		private boolean successful = true;
		private String errorMessage;
		private String operationType;

		public Builder sql(String sql) {
			this.sql = sql;
			return this;
		}

		public Builder parameters(List<Object> parameters) {
			this.parameters = parameters != null ? List.copyOf(parameters) : null;
			return this;
		}

		public Builder executionTimeMs(long executionTimeMs) {
			this.executionTimeMs = executionTimeMs;
			return this;
		}

		public Builder executedAt(Instant executedAt) {
			this.executedAt = executedAt;
			return this;
		}

		public Builder rowsAffected(int rowsAffected) {
			this.rowsAffected = rowsAffected;
			return this;
		}

		public Builder rowsReturned(int rowsReturned) {
			this.rowsReturned = rowsReturned;
			return this;
		}

		public Builder successful(boolean successful) {
			this.successful = successful;
			return this;
		}

		public Builder errorMessage(String errorMessage) {
			this.errorMessage = errorMessage;
			return this;
		}

		public Builder operationType(String operationType) {
			this.operationType = operationType;
			return this;
		}

		public QueryAuditLog build() {
			return new QueryAuditLog(
					sql,
					parameters,
					executionTimeMs,
					executedAt,
					rowsAffected,
					rowsReturned,
					successful,
					errorMessage,
					operationType
			);
		}
	}

	/**
	 * Create a new builder for QueryAuditLog.
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Check if this query execution was slow (exceeded threshold).
	 */
	public boolean isSlowQuery(long slowQueryThresholdMs) {
		return executionTimeMs > slowQueryThresholdMs;
	}

	/**
	 * Get formatted output for logging/debugging.
	 */
	@Override
	public String toString() {
		return """
				QueryAuditLog[
					sql='%s',
					executionTimeMs=%d,
					executedAt=%s,
					rowsAffected=%d,
					rowsReturned=%d,
					successful=%s,
					operationType='%s'
				]
				""".formatted(sql, executionTimeMs, executedAt, rowsAffected, rowsReturned, successful, operationType);
	}
}
