package org.springframework.boot.autoconfigure.jdbc;

import org.springframework.boot.autoconfigure.AutoConfiguration;

/**
 * Compatibility ordering bridge for third-party auto-configurations (e.g. holon-jdbc-spring-boot,
 * datasource-decorator) that still reference the Spring Boot 3.x DataSourceAutoConfiguration
 * type in {@code @AutoConfigureBefore}/{@code @AutoConfigureAfter} annotations.
 *
 * <p>Spring Boot 4 moved the real class to
 * {@code org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration}.
 * By registering this bridge as an {@code @AutoConfiguration} with proper ordering,
 * the Holon {@code DataSourcesAutoConfiguration} is guaranteed to execute before
 * Spring Boot 4's DataSourceAutoConfiguration, preserving the original semantics.</p>
 */
@AutoConfiguration(
        afterName = "com.holonplatform.jdbc.spring.boot.DataSourcesAutoConfiguration",
        before    = org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration.class
)
public final class DataSourceAutoConfiguration {

    private DataSourceAutoConfiguration() {
    }
}



