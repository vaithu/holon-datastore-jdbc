/*
 * Virtual Threads Connection Pool Implementation
 * 
 * Replaces traditional thread pool with Java 21+ Virtual Threads for 
 * efficient concurrent JDBC operations.
 * 
 * Benefits:
 * - Hundreds of thousands of concurrent connections
 * - Lower memory footprint per thread (~1-2KB vs 1MB)
 * - Simplified async code
 */

package com.holonplatform.datastore.jdbc.internal.concurrency;

import java.sql.Connection;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import javax.sql.DataSource;

/**
 * Wraps a traditional DataSource to execute blocking operations
 * (getConnection) using Virtual Threads, preventing platform thread starvation.
 * 
 * Usage:
 * <pre>
 * VirtualThreadDataSourceAdapter adapter = VirtualThreadDataSourceAdapter.builder(dataSource)
 *     .maxConnectionWaitMs(30000)
 *     .build();
 * 
 * Connection conn = adapter.getConnection();  // Runs on virtual thread
 * 
 * String result = adapter.executeOnVirtualThread(connection -> {
 *     // Blocking I/O runs on virtual thread
 *     return performQuery(connection);
 * });
 * </pre>
 */
public class VirtualThreadDataSourceAdapter implements AutoCloseable {

    private final DataSource dataSource;
    private final ExecutorService virtualExecutor;
    private final int maxConnectionWaitMs;
    private volatile boolean closed;

    /**
     * Get a builder to create a VirtualThreadDataSourceAdapter instance.
     * @param dataSource the underlying DataSource to wrap (not null)
     * @return adapter builder
     */
    public static Builder builder(DataSource dataSource) {
        return new DefaultBuilder(dataSource);
    }

    /**
     * VirtualThreadDataSourceAdapter builder interface.
     */
    public interface Builder {
        /**
         * Set the maximum time to wait for connection acquisition.
         * @param maxConnectionWaitMs timeout in milliseconds
         * @return this builder
         */
        Builder maxConnectionWaitMs(int maxConnectionWaitMs);

        /**
         * Build the VirtualThreadDataSourceAdapter.
         * @return configured adapter instance
         */
        VirtualThreadDataSourceAdapter build();
    }

    /**
     * Internal constructor used by builder.
     */
    private VirtualThreadDataSourceAdapter(DataSource dataSource, int maxConnectionWaitMs, boolean internal) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.maxConnectionWaitMs = maxConnectionWaitMs;
        this.closed = false;
        
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Acquires a connection using a virtual thread.
     * 
     * This prevents blocking platform threads when the connection pool is slow,
     * which is critical for high-concurrency scenarios.
     * 
     * @return a Connection acquired on a virtual thread
     * @throws Exception if connection cannot be acquired within timeout
     */
    public Connection getConnection() throws Exception {
        if (closed) {
            throw new IllegalStateException("VirtualThreadDataSourceAdapter is closed");
        }
        
        var future = virtualExecutor.submit((java.util.concurrent.Callable<Connection>) dataSource::getConnection);
        try {
            return future.get(maxConnectionWaitMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException("Connection acquisition timeout after " + 
                maxConnectionWaitMs + "ms", e);
        }
    }

    /**
     * Executes a connection operation on a virtual thread.
     * 
     * The connection is automatically closed after the operation completes.
     * 
     * Pattern:
     * <pre>
     * String result = adapter.executeOnVirtualThread(connection -> {
     *     PreparedStatement stmt = connection.prepareStatement("SELECT ...");
     *     // execute query
     *     return extractResult(stmt);
     * });
     * </pre>
     * 
     * @param <T> the return type
     * @param operation the operation to execute on a virtual thread
     * @return the result of the operation
     * @throws Exception if the operation fails or times out
     */
    public <T> T executeOnVirtualThread(ConnectionOperation<T> operation) throws Exception {
        if (closed) {
            throw new IllegalStateException("VirtualThreadDataSourceAdapter is closed");
        }
        
        var future = virtualExecutor.submit((java.util.concurrent.Callable<T>) () -> {
            try (Connection conn = dataSource.getConnection()) {
                return operation.execute(conn);
            }
        });
        try {
            return future.get(maxConnectionWaitMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException("Operation timeout on virtual thread after " + 
                maxConnectionWaitMs + "ms", e);
        }
    }

    /**
     * Functional interface for connection operations.
     * 
     * @param <T> the return type
     */
    @FunctionalInterface
    public interface ConnectionOperation<T> {
        /**
         * Execute an operation on the connection.
         * 
         * @param conn the connection
         * @return the operation result
         * @throws Exception if the operation fails
         */
        T execute(Connection conn) throws Exception;
    }

    /**
     * Shuts down the virtual thread executor.
     */
    public void shutdown() {
        closed = true;
        virtualExecutor.shutdown();
    }

    /**
     * Closes the adapter and shuts down the virtual thread executor.
     */
    @Override
    public void close() {
        shutdown();
    }

    /**
     * Default builder implementation for VirtualThreadDataSourceAdapter.
     */
    private static final class DefaultBuilder implements Builder {
        private final DataSource dataSource;
        private int maxConnectionWaitMs = 30000;

        DefaultBuilder(DataSource dataSource) {
            this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        }

        @Override
        public Builder maxConnectionWaitMs(int maxConnectionWaitMs) {
            this.maxConnectionWaitMs = maxConnectionWaitMs;
            return this;
        }

        @Override
        public VirtualThreadDataSourceAdapter build() {
            return new VirtualThreadDataSourceAdapter(dataSource, maxConnectionWaitMs, true);
        }
    }
}
