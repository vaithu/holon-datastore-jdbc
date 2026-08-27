package org.springframework.boot.autoconfigure.jdbc;

import org.springframework.boot.autoconfigure.AutoConfiguration;

/**
 * Compatibility ordering bridge for third-party auto-configurations (e.g. holon-jdbc-spring-boot)
 * that reference the Spring Boot 3.x DataSourceTransactionManagerAutoConfiguration type in
 * {@code @AutoConfigureBefore} annotations.
 *
 * <p>Spring Boot 4 moved the real class to
 * {@code org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration}.
 * This bridge ensures correct ordering: Holon DataSources before Spring Boot 4's transaction
 * manager auto-configuration.</p>
 */
@AutoConfiguration(
        afterName = "com.holonplatform.jdbc.spring.boot.DataSourcesAutoConfiguration",
        before    = org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration.class
)
public final class DataSourceTransactionManagerAutoConfiguration {

    private DataSourceTransactionManagerAutoConfiguration() {
    }
}


