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
package com.holonplatform.datastore.jdbc.internal.batch;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable record capturing results of a parallel batch operation.
 * 
 * Provides comprehensive metrics about batch execution including:
 * - Total rows processed and success/failure counts
 * - Execution time and throughput
 * - Error details for failed operations
 * 
 * @since 11.1.0
 */
public record ParallelBatchResult(
		int totalRows,
		int successfulRows,
		int failedRows,
		long executionTimeMs,
		List<BatchOperationError> errors,
		int degreeOfParallelism
) {

	/**
	 * Constructor with validation.
	 */
	public ParallelBatchResult {
		Objects.requireNonNull(errors, "errors cannot be null");
		errors = Collections.unmodifiableList(errors);
	}

	/**
	 * Calculate success rate as percentage.
	 */
	public double getSuccessRate() {
		if (totalRows == 0) {
			return 0.0;
		}
		return (double) successfulRows / totalRows * 100.0;
	}

	/**
	 * Calculate throughput (rows per second).
	 */
	public double getThroughputPerSecond() {
		if (executionTimeMs == 0) {
			return 0.0;
		}
		return (double) totalRows / (executionTimeMs / 1000.0);
	}

	/**
	 * Check if operation was fully successful.
	 */
	public boolean isFullySuccessful() {
		return failedRows == 0 && errors.isEmpty();
	}

	/**
	 * Check if operation had any failures.
	 */
	public boolean hasErrors() {
		return failedRows > 0 || !errors.isEmpty();
	}

	/**
	 * Get average time per row (milliseconds).
	 */
	public double getAverageTimePerRow() {
		if (totalRows == 0) {
			return 0.0;
		}
		return (double) executionTimeMs / totalRows;
	}

	/**
	 * Get formatted summary of batch operation results.
	 */
	public String getSummary() {
		return """
				Parallel Batch Results:
				  Total rows: %d
				  Successful: %d (%.1f%%)
				  Failed: %d
				  Execution time: %d ms
				  Throughput: %.2f rows/sec
				  Avg time per row: %.2f ms
				  Degree of parallelism: %d
				""".formatted(
				totalRows,
				successfulRows,
				getSuccessRate(),
				failedRows,
				executionTimeMs,
				getThroughputPerSecond(),
				getAverageTimePerRow(),
				degreeOfParallelism
		);
	}

	/**
	 * Record representing an error that occurred during a batch operation.
	 */
	public record BatchOperationError(
			int rowIndex,
			String rowData,
			String errorMessage,
			Exception exception
	) {
		/**
		 * Constructor with validation.
		 */
		public BatchOperationError {
			Objects.requireNonNull(errorMessage, "errorMessage cannot be null");
		}
	}

	/**
	 * Builder for creating ParallelBatchResult instances.
	 */
	public static class Builder {
		private int totalRows;
		private int successfulRows;
		private int failedRows;
		private long executionTimeMs;
		private List<BatchOperationError> errors = Collections.emptyList();
		private int degreeOfParallelism;

		public Builder totalRows(int totalRows) {
			this.totalRows = totalRows;
			return this;
		}

		public Builder successfulRows(int successfulRows) {
			this.successfulRows = successfulRows;
			return this;
		}

		public Builder failedRows(int failedRows) {
			this.failedRows = failedRows;
			return this;
		}

		public Builder executionTimeMs(long executionTimeMs) {
			this.executionTimeMs = executionTimeMs;
			return this;
		}

		public Builder errors(List<BatchOperationError> errors) {
			this.errors = errors != null ? List.copyOf(errors) : Collections.emptyList();
			return this;
		}

		public Builder degreeOfParallelism(int degreeOfParallelism) {
			this.degreeOfParallelism = degreeOfParallelism;
			return this;
		}

		public ParallelBatchResult build() {
			return new ParallelBatchResult(
					totalRows,
					successfulRows,
					failedRows,
					executionTimeMs,
					errors,
					degreeOfParallelism
			);
		}
	}

	/**
	 * Create a new builder for ParallelBatchResult.
	 */
	public static Builder builder() {
		return new Builder();
	}
}
