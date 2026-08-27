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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of QueryAuditListener that detects and logs slow queries.
 * 
 * This listener monitors query execution time and logs queries that exceed
 * the configured threshold. Useful for performance monitoring and optimization.
 * 
 * @since 11.1.0
 */
public final class SlowQueryDetector implements QueryAuditListener {

	private static final Logger LOGGER = Logger.getLogger(SlowQueryDetector.class.getName());

	private final long slowQueryThresholdMs;
	private final boolean logParameters;
	private final List<QueryAuditLog> slowQueries;
	private final int maxStoredQueries;

	/**
	 * Create a SlowQueryDetector with the given threshold.
	 * 
	 * @param slowQueryThresholdMs the threshold in milliseconds above which queries are considered slow
	 */
	public SlowQueryDetector(long slowQueryThresholdMs) {
		this(slowQueryThresholdMs, true, 1000);
	}

	/**
	 * Create a SlowQueryDetector with detailed configuration.
	 * 
	 * @param slowQueryThresholdMs the threshold in milliseconds
	 * @param logParameters whether to log SQL parameters
	 * @param maxStoredQueries maximum number of slow queries to keep in memory
	 */
	public SlowQueryDetector(long slowQueryThresholdMs, boolean logParameters, int maxStoredQueries) {
		this.slowQueryThresholdMs = slowQueryThresholdMs;
		this.logParameters = logParameters;
		this.slowQueries = Collections.synchronizedList(new ArrayList<>(maxStoredQueries));
		this.maxStoredQueries = maxStoredQueries;
	}

	@Override
	public void onQueryExecuted(QueryAuditLog auditLog) {
		Objects.requireNonNull(auditLog, "auditLog cannot be null");

		if (auditLog.isSlowQuery(slowQueryThresholdMs)) {
			logSlowQuery(auditLog);
			storeSlowQuery(auditLog);
		}
	}

	/**
	 * Log a slow query at WARNING level.
	 */
	private void logSlowQuery(QueryAuditLog auditLog) {
		String message = formatSlowQueryMessage(auditLog);
		LOGGER.log(Level.WARNING, message);
	}

	/**
	 * Format slow query message for logging.
	 */
	private String formatSlowQueryMessage(QueryAuditLog auditLog) {
		StringBuilder msg = new StringBuilder();
		msg.append("Slow query detected (").append(auditLog.executionTimeMs()).append("ms > ")
				.append(slowQueryThresholdMs).append("ms): ");
		msg.append(auditLog.sql());

		if (logParameters && auditLog.parameters() != null && !auditLog.parameters().isEmpty()) {
			msg.append(" [Parameters: ").append(auditLog.parameters()).append("]");
		}

		msg.append(" [Type: ").append(auditLog.operationType()).append("]");

		if (auditLog.rowsAffected() > 0) {
			msg.append(" [Rows affected: ").append(auditLog.rowsAffected()).append("]");
		}
		if (auditLog.rowsReturned() > 0) {
			msg.append(" [Rows returned: ").append(auditLog.rowsReturned()).append("]");
		}

		return msg.toString();
	}

	/**
	 * Store slow query in memory (bounded list).
	 */
	private void storeSlowQuery(QueryAuditLog auditLog) {
		slowQueries.add(auditLog);

		// Remove oldest if we exceed max
		if (slowQueries.size() > maxStoredQueries) {
			slowQueries.remove(0);
		}
	}

	/**
	 * Get all detected slow queries.
	 * 
	 * @return unmodifiable list of slow queries
	 */
	public List<QueryAuditLog> getSlowQueries() {
		return Collections.unmodifiableList(new ArrayList<>(slowQueries));
	}

	/**
	 * Get count of detected slow queries.
	 */
	public int getSlowQueryCount() {
		return slowQueries.size();
	}

	/**
	 * Clear the slow query history.
	 */
	public void clearHistory() {
		slowQueries.clear();
	}

	/**
	 * Get the average execution time of slow queries.
	 */
	public double getAverageSlowQueryTime() {
		if (slowQueries.isEmpty()) {
			return 0.0;
		}
		long totalTime = slowQueries.stream().mapToLong(QueryAuditLog::executionTimeMs).sum();
		return (double) totalTime / slowQueries.size();
	}

	/**
	 * Get the slowest query execution time.
	 */
	public long getMaxSlowQueryTime() {
		return slowQueries.stream()
				.mapToLong(QueryAuditLog::executionTimeMs)
				.max()
				.orElse(0L);
	}

	/**
	 * Get statistics about slow queries as a formatted string.
	 */
	public String getStatistics() {
		return """
				Slow Query Statistics:
				  Total slow queries: %d
				  Average execution time: %.2f ms
				  Max execution time: %d ms
				  Threshold: %d ms
				""".formatted(
				getSlowQueryCount(),
				getAverageSlowQueryTime(),
				getMaxSlowQueryTime(),
				slowQueryThresholdMs
		);
	}
}
