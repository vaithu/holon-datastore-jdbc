/*
 * Virtual Thread Configuration Properties
 */

package com.holonplatform.datastore.jdbc.spring.boot.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Virtual Thread-based DataSource adapter.
 * 
 * Properties can be set via application.yml:
 * <pre>
 * holon:
 *   datastore:
 *     jdbc:
 *       virtual-threads:
 *         enabled: true
 *         wrap-datasource: false
 *         max-connection-wait-ms: 30000
 * </pre>
 */
@ConfigurationProperties(prefix = "holon.datastore.jdbc.virtual-threads")
public class VirtualThreadProperties {

    /**
     * Enable virtual thread support for JDBC operations.
     * Default: false (opt-in)
     */
    private boolean enabled = false;

    /**
     * Wrap the primary DataSource with virtual thread adapter.
     * Only applies if enabled=true
     * Default: false
     */
    private boolean wrapDatasource = false;

    /**
     * Maximum wait time for connection acquisition in milliseconds.
     * Default: 30000 (30 seconds)
     */
    private int maxConnectionWaitMs = 30000;

    public VirtualThreadProperties() {
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isWrapDatasource() {
        return wrapDatasource;
    }

    public void setWrapDatasource(boolean wrapDatasource) {
        this.wrapDatasource = wrapDatasource;
    }

    public int getMaxConnectionWaitMs() {
        return maxConnectionWaitMs;
    }

    public void setMaxConnectionWaitMs(int maxConnectionWaitMs) {
        this.maxConnectionWaitMs = maxConnectionWaitMs;
    }

    @Override
    public String toString() {
        return "VirtualThreadProperties{" +
                "enabled=" + enabled +
                ", wrapDatasource=" + wrapDatasource +
                ", maxConnectionWaitMs=" + maxConnectionWaitMs +
                '}';
    }
}
