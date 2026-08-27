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

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

import com.holonplatform.datastore.jdbc.internal.audit.QueryAuditListener;
import com.holonplatform.datastore.jdbc.internal.audit.SlowQueryDetector;

/**
 * Spring Boot auto-configuration for query auditing.
 * 
 * Automatically enables query auditing when the holon.datastore.jdbc.auditing.enabled
 * property is set to true.
 * 
 * Provides beans for:
 * - QueryAuditListener: the audit listener interface
 * - SlowQueryDetector: implementation that detects slow queries
 * 
 * Usage:
 * 
 * <pre>
 * holon:
 *   datastore:
 *     jdbc:
 *       auditing:
 *         enabled: true
 *         slow-query-threshold-ms: 500
 *         log-parameters: true
 * </pre>
 * 
 * @since 11.1.0
 */
@AutoConfiguration
@EnableConfigurationProperties(QueryAuditingProperties.class)
public final class QueryAuditingAutoConfiguration {

	/**
	 * Create the slow query detector bean.
	 */
	@Bean
	@Conditional(QueryAuditingEnabledCondition.class)
	public QueryAuditListener queryAuditListener(QueryAuditingProperties properties) {
		return new SlowQueryDetector(
				properties.getSlowQueryThresholdMs(),
				properties.isLogParameters(),
				properties.getMaxStoredQueries()
		);
	}

	/**
	 * Conditional that checks if auditing is enabled.
	 */
	public static final class QueryAuditingEnabledCondition implements org.springframework.context.annotation.Condition {
		@Override
		public boolean matches(
				org.springframework.context.annotation.ConditionContext context,
				org.springframework.core.type.AnnotatedTypeMetadata metadata
		) {
			String enabled = context.getEnvironment().getProperty("holon.datastore.jdbc.auditing.enabled");
			return "true".equalsIgnoreCase(enabled);
		}
	}
}
