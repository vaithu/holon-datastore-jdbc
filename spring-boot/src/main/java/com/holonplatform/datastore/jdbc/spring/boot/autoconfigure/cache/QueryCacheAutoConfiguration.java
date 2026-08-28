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
package com.holonplatform.datastore.jdbc.spring.boot.autoconfigure.cache;

import com.holonplatform.datastore.jdbc.internal.cache.CachingInterceptor;
import com.holonplatform.datastore.jdbc.internal.cache.QueryResultCache;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration for query result caching.
 * Enables in-memory caching with configurable TTL and max size.
 *
 * Configuration properties (application.yml/properties):
 * <pre>
 * holon:
 *   datastore:
 *     jdbc:
 *       cache:
 *         enabled: true
 *         max-size: 1000
 *         ttl-millis: 300000
 * </pre>
 *
 * @since 11.2
 */
@AutoConfiguration
@EnableConfigurationProperties(QueryCacheProperties.class)
public class QueryCacheAutoConfiguration {

    /**
     * Create QueryResultCache bean with properties from application.yml/properties.
     *
     * @param properties cache configuration properties
     * @return QueryResultCache bean
     */
    @Bean
    @ConditionalOnMissingBean
    public QueryResultCache queryResultCache(QueryCacheProperties properties) {
        return new QueryResultCache(
                properties.getMaxSize(),
                properties.getTtlMillis()
        );
    }

    /**
     * Create CachingInterceptor bean for mutation handling.
     *
     * @param cache the query result cache
     * @return CachingInterceptor bean
     */
    @Bean
    @ConditionalOnMissingBean
    public CachingInterceptor cachingInterceptor(QueryResultCache cache) {
        return new CachingInterceptor(cache);
    }
}

/**
 * Configuration properties for query result caching.
 */
@ConfigurationProperties(prefix = "holon.datastore.jdbc.cache")
class QueryCacheProperties {

    private boolean enabled = true;
    private int maxSize = 1000;
    private long ttlMillis = 5 * 60 * 1000; // 5 minutes

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public long getTtlMillis() {
        return ttlMillis;
    }

    public void setTtlMillis(long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }

    @Override
    public String toString() {
        return String.format(
                "QueryCacheProperties{enabled=%s, maxSize=%d, ttlMillis=%d}",
                enabled, maxSize, ttlMillis
        );
    }
}
