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
package com.holonplatform.datastore.jdbc.spring.boot.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for query auditing functionality.
 * 
 * Properties can be configured via application.yml or application.properties:
 * 
 * <pre>
 * holon:
 *   datastore:
 *     jdbc:
 *       auditing:
 *         enabled: true
 *         slow-query-threshold-ms: 1000
 *         log-parameters: true
 *         max-stored-queries: 1000
 * </pre>
 * 
 * @since 11.1.0
 */
@Component
@ConfigurationProperties(prefix = "holon.datastore.jdbc.auditing")
public final class QueryAuditingProperties {

	/**
	 * Enable query auditing
	 */
	private boolean enabled = false;

	/**
	 * Threshold in milliseconds for slow query detection
	 */
	private long slowQueryThresholdMs = 1000;

	/**
	 * Whether to log SQL parameters in audit logs
	 */
	private boolean logParameters = true;

	/**
	 * Maximum number of slow queries to store in memory
	 */
	private int maxStoredQueries = 1000;

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public long getSlowQueryThresholdMs() {
		return slowQueryThresholdMs;
	}

	public void setSlowQueryThresholdMs(long slowQueryThresholdMs) {
		this.slowQueryThresholdMs = slowQueryThresholdMs;
	}

	public boolean isLogParameters() {
		return logParameters;
	}

	public void setLogParameters(boolean logParameters) {
		this.logParameters = logParameters;
	}

	public int getMaxStoredQueries() {
		return maxStoredQueries;
	}

	public void setMaxStoredQueries(int maxStoredQueries) {
		this.maxStoredQueries = maxStoredQueries;
	}
}
