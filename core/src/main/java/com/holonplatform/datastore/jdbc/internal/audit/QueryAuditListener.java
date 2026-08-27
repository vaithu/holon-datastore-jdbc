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

/**
 * Listener interface for SQL query execution auditing and monitoring.
 * 
 * Implementations can track queries for purposes like:
 * - Performance monitoring (slow query detection)
 * - Query statistics and analytics
 * - Compliance and audit trail logging
 * - Security monitoring
 * 
 * @since 11.1.0
 */
@FunctionalInterface
public interface QueryAuditListener {

	/**
	 * Called when a SQL query has been executed.
	 * 
	 * @param auditLog the audit log entry containing query details
	 */
	void onQueryExecuted(QueryAuditLog auditLog);

	/**
	 * Called when a SQL query execution fails.
	 * 
	 * Default implementation delegates to onQueryExecuted.
	 * 
	 * @param auditLog the audit log entry with error details
	 * @param exception the exception that occurred
	 */
	default void onQueryFailed(QueryAuditLog auditLog, Exception exception) {
		onQueryExecuted(auditLog);
	}
}
