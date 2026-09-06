/*
 * Virtual Thread DataSource Auto-Configuration for Spring Boot
 */

package com.holonplatform.datastore.jdbc.spring.boot.config;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import com.holonplatform.datastore.jdbc.internal.concurrency.VirtualThreadDataSourceAdapter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for Virtual Thread-based DataSource wrapper.
 * 
 * This configuration activates when:
 * - holon.datastore.jdbc.virtual-threads.enabled=true
 * - Running on Java 21+ (Virtual Threads available)
 * 
 * Impact:
 * - Prevents platform thread starvation in high-concurrency scenarios
 * - Enables thousands of concurrent JDBC operations
 * - Reduces memory footprint per concurrent operation
 * 
 * Usage:
 * <pre>
 * # Enable in application.yml:
 * holon:
 *   datastore:
 *     jdbc:
 *       virtual-threads:
 *         enabled: true
 *         wrap-datasource: true
 *         max-connection-wait-ms: 30000
 * </pre>
 */
@AutoConfiguration
@EnableConfigurationProperties(VirtualThreadProperties.class)
@ConditionalOnClass(VirtualThreadDataSourceAdapter.class)
@ConditionalOnProperty(
    name = "holon.datastore.jdbc.virtual-threads.enabled",
    havingValue = "true"
)
public class VirtualThreadDataSourceAutoConfiguration {

    /**
     * Creates the VirtualThreadDataSourceAdapter bean.
     * 
     * This bean wraps any DataSource and executes connection acquisition
     * on virtual threads, preventing blocking of platform threads.
     * 
     * @param dataSource the primary DataSource to wrap
     * @param props virtual thread configuration properties
     * @return VirtualThreadDataSourceAdapter bean
     */
    @Bean
    public VirtualThreadDataSourceAdapter virtualThreadDataSourceAdapter(
            DataSource dataSource,
            VirtualThreadProperties props) {
        return VirtualThreadDataSourceAdapter.builder(dataSource)
            .maxConnectionWaitMs(props.getMaxConnectionWaitMs())
            .build();
    }

    /**
     * Optionally wraps the primary DataSource with the virtual thread adapter.
     * 
     * Only created when holon.datastore.jdbc.virtual-threads.wrap-datasource=true
     * 
     * This DataSource is suitable for direct use with the JdbcDatastore:
     * <pre>
     * JdbcDatastore.builder().dataSource(wrappedDataSource).build();
     * </pre>
     * 
     * @param adapter the virtual thread adapter
     * @return wrapped DataSource bean
     */
    @Bean
    @ConditionalOnProperty(
        name = "holon.datastore.jdbc.virtual-threads.wrap-datasource",
        havingValue = "true"
    )
    public DataSource wrappedDataSource(VirtualThreadDataSourceAdapter adapter) {
        return new DataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                try {
                    return adapter.getConnection();
                } catch (SQLException e) {
                    throw e;
                } catch (Exception e) {
                    throw new SQLException("Virtual thread connection failed", e);
                }
            }

            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                throw new SQLException("Virtual thread adapter doesn't support user/password authentication");
            }

            @Override
            public PrintWriter getLogWriter() throws SQLException {
                return null;
            }

            @Override
            public void setLogWriter(PrintWriter out) throws SQLException {
            }

            @Override
            public void setLoginTimeout(int seconds) throws SQLException {
            }

            @Override
            public int getLoginTimeout() throws SQLException {
                return 0;
            }

            @Override
            public Logger getParentLogger() throws SQLFeatureNotSupportedException {
                throw new SQLFeatureNotSupportedException();
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                if (iface.isInstance(this)) {
                    return iface.cast(this);
                }
                throw new SQLException("Not a wrapper of " + iface.getName());
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return iface.isInstance(this);
            }
        };
    }
}
