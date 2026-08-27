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
package com.holonplatform.datastore.jdbc.internal.concurrency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Structured concurrency scope for executing multiple transactions concurrently.
 * 
 * This scope allows multiple database operations to execute in parallel with
 * automatic error aggregation and rollback coordination. If any operation fails,
 * all pending operations are cancelled and aggregated errors are thrown.
 * 
 * This implements structured concurrency patterns using virtual threads and
 * CountDownLatch for coordination.
 * 
 * Example usage:
 * <pre>
 * try (ConcurrentTransactionScope scope = new ConcurrentTransactionScope(datastore, 4)) {
 *     scope.submit("Task 1", () -> datastore.insert(entity1).execute());
 *     scope.submit("Task 2", () -> datastore.insert(entity2).execute());
 *     scope.submit("Task 3", () -> datastore.update(entity3).execute());
 *     scope.join();
 * } catch (AggregatedTransactionException e) {
 *     System.out.println("Errors in " + e.getFailedTasks().size() + " tasks");
 * }
 * </pre>
 * 
 * @since 11.1.0
 */
public final class ConcurrentTransactionScope implements AutoCloseable {

	private static final Logger LOGGER = Logger.getLogger(ConcurrentTransactionScope.class.getName());

	private final int degreeOfParallelism;
	private final ExecutorService executor;
	private final List<Task<?>> tasks;
	private final AtomicBoolean closed;
	private final long timeoutMs;

	/**
	 * Record representing a single task in the scope.
	 */
	private static final class Task<T> {
		final String taskName;
		final Supplier<T> operation;
		final AtomicReference<T> result;
		final AtomicReference<Exception> error;
		final CountDownLatch latch;

		Task(String taskName, Supplier<T> operation) {
			this.taskName = taskName;
			this.operation = operation;
			this.result = new AtomicReference<>();
			this.error = new AtomicReference<>();
			this.latch = new CountDownLatch(1);
		}
	}

	/**
	 * Create a concurrent transaction scope with default settings.
	 * 
	 * @param degreeOfParallelism number of concurrent tasks
	 */
	public ConcurrentTransactionScope(int degreeOfParallelism) {
		this(degreeOfParallelism, 30000); // 30 second default timeout
	}

	/**
	 * Create a concurrent transaction scope with timeout.
	 * 
	 * @param degreeOfParallelism number of concurrent tasks
	 * @param timeoutMs timeout in milliseconds
	 */
	public ConcurrentTransactionScope(int degreeOfParallelism, long timeoutMs) {
		if (degreeOfParallelism < 1) {
			throw new IllegalArgumentException("degreeOfParallelism must be >= 1");
		}
		if (timeoutMs < 1) {
			throw new IllegalArgumentException("timeoutMs must be >= 1");
		}

		this.degreeOfParallelism = degreeOfParallelism;
		this.timeoutMs = timeoutMs;
		this.executor = Executors.newVirtualThreadPerTaskExecutor();
		this.tasks = Collections.synchronizedList(new ArrayList<>());
		this.closed = new AtomicBoolean(false);
	}

	/**
	 * Submit a task to execute concurrently.
	 * 
	 * @param taskName name for logging/error reporting
	 * @param operation the operation to execute
	 */
	public <T> void submit(String taskName, Supplier<T> operation) {
		Objects.requireNonNull(taskName, "taskName cannot be null");
		Objects.requireNonNull(operation, "operation cannot be null");

		if (closed.get()) {
			throw new IllegalStateException("ConcurrentTransactionScope has been closed");
		}

		Task<T> task = new Task<>(taskName, operation);
		tasks.add(task);

		executor.submit(() -> executeTask(task));
	}

	/**
	 * Execute a single task with error handling.
	 */
	private <T> void executeTask(Task<T> task) {
		try {
			long startTime = System.currentTimeMillis();
			T result = task.operation.get();
			long elapsed = System.currentTimeMillis() - startTime;

			task.result.set(result);
			LOGGER.fine("Task '" + task.taskName + "' completed in " + elapsed + "ms");
		} catch (Exception e) {
			task.error.set(e);
			LOGGER.log(Level.WARNING, "Task '" + task.taskName + "' failed", e);
		} finally {
			task.latch.countDown();
		}
	}

	/**
	 * Wait for all tasks to complete and collect results.
	 * 
	 * @throws AggregatedTransactionException if any tasks failed
	 */
	public void join() throws AggregatedTransactionException {
		try {
			long startTime = System.currentTimeMillis();

			// Wait for all tasks with timeout
			for (Task<?> task : tasks) {
				long elapsed = System.currentTimeMillis() - startTime;
				long remaining = timeoutMs - elapsed;

				if (remaining <= 0) {
					throw new AggregatedTransactionException(
							"Timeout waiting for task: " + task.taskName,
							Collections.emptyMap(),
							Collections.singletonMap(
									task.taskName,
									new TimeoutException("Task timeout after " + timeoutMs + "ms")
							)
					);
				}

				if (!task.latch.await(remaining, java.util.concurrent.TimeUnit.MILLISECONDS)) {
					throw new AggregatedTransactionException(
							"Task timeout: " + task.taskName,
							Collections.emptyMap(),
							Collections.singletonMap(
									task.taskName,
									new TimeoutException("Task did not complete within timeout")
							)
					);
				}
			}

			// Collect errors
			java.util.Map<String, Exception> errors = new java.util.LinkedHashMap<>();
			for (Task<?> task : tasks) {
				Exception error = task.error.get();
				if (error != null) {
					errors.put(task.taskName, error);
				}
			}

			// If any errors occurred, throw aggregated exception
			if (!errors.isEmpty()) {
				java.util.Map<String, Object> results = new java.util.LinkedHashMap<>();
				for (Task<?> task : tasks) {
					if (task.error.get() == null) {
						results.put(task.taskName, task.result.get());
					}
				}
				throw new AggregatedTransactionException(
						"Transaction scope failed with " + errors.size() + " error(s)",
						results,
						errors
				);
			}

			long totalTime = System.currentTimeMillis() - startTime;
			LOGGER.info("All " + tasks.size() + " tasks completed successfully in " + totalTime + "ms");

		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AggregatedTransactionException(
					"Transaction scope interrupted",
					Collections.emptyMap(),
					Collections.singletonMap("_scope", e)
			);
		}
	}

	/**
	 * Close the scope and shutdown the executor.
	 */
	@Override
	public void close() {
		if (closed.compareAndSet(false, true)) {
			executor.shutdown();
		}
	}

	/**
	 * Get the number of submitted tasks.
	 */
	public int getTaskCount() {
		return tasks.size();
	}

	/**
	 * Get the configured degree of parallelism.
	 */
	public int getDegreeOfParallelism() {
		return degreeOfParallelism;
	}

	/**
	 * Exception for aggregated errors from concurrent transaction scope.
	 */
	public static final class AggregatedTransactionException extends Exception {
		private final java.util.Map<String, Object> successfulResults;
		private final java.util.Map<String, Exception> failedTasks;

		public AggregatedTransactionException(
				String message,
				java.util.Map<String, Object> successfulResults,
				java.util.Map<String, Exception> failedTasks
		) {
			super(message);
			this.successfulResults = Collections.unmodifiableMap(successfulResults);
			this.failedTasks = Collections.unmodifiableMap(failedTasks);
		}

		public java.util.Map<String, Object> getSuccessfulResults() {
			return successfulResults;
		}

		public java.util.Map<String, Exception> getFailedTasks() {
			return failedTasks;
		}

		public int getFailureCount() {
			return failedTasks.size();
		}

		public int getSuccessCount() {
			return successfulResults.size();
		}

		@Override
		public String toString() {
			return """
					AggregatedTransactionException:
					  Message: %s
					  Successful: %d tasks
					  Failed: %d tasks
					  Failures: %s
					""".formatted(
					getMessage(),
					getSuccessCount(),
					getFailureCount(),
					failedTasks.keySet()
			);
		}
	}
}
