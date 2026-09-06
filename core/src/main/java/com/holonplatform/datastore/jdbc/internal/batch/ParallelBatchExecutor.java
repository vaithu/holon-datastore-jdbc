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
package com.holonplatform.datastore.jdbc.internal.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executor for parallel batch operations using virtual threads.
 * 
 * This executor splits a large batch into smaller partitions and processes
 * each partition concurrently using virtual threads, achieving significant
 * performance improvements over sequential processing.
 * 
 * Usage:
 * <pre>
 * List<MyEntity> data = ...;
 * ParallelBatchExecutor<MyEntity> executor = ParallelBatchExecutor.builder()
 *     .degreeOfParallelism(4)
 *     .partitionSize(100)
 *     .build();
 * ParallelBatchResult result = executor.execute(data, entity -> {
 *     datastore.insert(entity).execute();
 * });
 * System.out.println(result.getSummary());
 * </pre>
 * 
 * @since 11.1.0
 */
public final class ParallelBatchExecutor<T> {

	private static final Logger LOGGER = Logger.getLogger(ParallelBatchExecutor.class.getName());

	private final int degreeOfParallelism;
	private final int partitionSize;
	private final ExecutorService executor;

	/**
	 * Get a builder to create a ParallelBatchExecutor instance.
	 * @return executor builder
	 */
	public static Builder builder() {
		return new DefaultBuilder();
	}

	/**
	 * ParallelBatchExecutor builder interface.
	 */
	public interface Builder {
		/**
		 * Set the degree of parallelism.
		 * @param degreeOfParallelism number of parallel threads (must be >= 1)
		 * @return this builder
		 */
		Builder degreeOfParallelism(int degreeOfParallelism);

		/**
		 * Set the partition size.
		 * @param partitionSize size of each partition batch (must be >= 1)
		 * @return this builder
		 */
		Builder partitionSize(int partitionSize);

		/**
		 * Build the ParallelBatchExecutor.
		 * @return configured executor instance
		 */
		<T> ParallelBatchExecutor<T> build();
	}

	/**
	 * Internal constructor used by builder.
	 */
	private ParallelBatchExecutor(int degreeOfParallelism, int partitionSize, boolean internal) {
		if (degreeOfParallelism < 1) {
			throw new IllegalArgumentException("degreeOfParallelism must be >= 1");
		}
		if (partitionSize < 1) {
			throw new IllegalArgumentException("partitionSize must be >= 1");
		}

		this.degreeOfParallelism = degreeOfParallelism;
		this.partitionSize = partitionSize;
		this.executor = Executors.newVirtualThreadPerTaskExecutor();
	}

	/**
	 * Execute a batch operation in parallel.
	 * 
	 * @param items the items to process
	 * @param operation the operation to apply to each item
	 * @return batch execution result with metrics
	 */
	public ParallelBatchResult execute(List<T> items, Consumer<T> operation) {
		Objects.requireNonNull(items, "items cannot be null");
		Objects.requireNonNull(operation, "operation cannot be null");

		if (items.isEmpty()) {
			return ParallelBatchResult.builder()
					.totalRows(0)
					.successfulRows(0)
					.failedRows(0)
					.executionTimeMs(0)
					.errors(Collections.emptyList())
					.degreeOfParallelism(degreeOfParallelism)
					.build();
		}

		long startTime = System.currentTimeMillis();

		// Partition the work
		List<List<T>> partitions = partitionBatch(items);

		// Counters for tracking results
		AtomicInteger successCount = new AtomicInteger(0);
		AtomicInteger failureCount = new AtomicInteger(0);
		List<ParallelBatchResult.BatchOperationError> errors = Collections
				.synchronizedList(new ArrayList<>());

		// Execute partitions in parallel
		executePartitionsInParallel(partitions, operation, successCount, failureCount, errors);

		long endTime = System.currentTimeMillis();
		long executionTimeMs = endTime - startTime;

		return ParallelBatchResult.builder()
				.totalRows(items.size())
				.successfulRows(successCount.get())
				.failedRows(failureCount.get())
				.executionTimeMs(executionTimeMs)
				.errors(new ArrayList<>(errors))
				.degreeOfParallelism(degreeOfParallelism)
				.build();
	}

	/**
	 * Partition a batch into smaller chunks for parallel processing.
	 */
	private List<List<T>> partitionBatch(List<T> items) {
		List<List<T>> partitions = new ArrayList<>();
		for (int i = 0; i < items.size(); i += partitionSize) {
			int endIdx = Math.min(i + partitionSize, items.size());
			partitions.add(new ArrayList<>(items.subList(i, endIdx)));
		}
		return partitions;
	}

	/**
	 * Execute all partitions in parallel using a thread pool.
	 */
	private void executePartitionsInParallel(
			List<List<T>> partitions,
			Consumer<T> operation,
			AtomicInteger successCount,
			AtomicInteger failureCount,
			List<ParallelBatchResult.BatchOperationError> errors
	) {
		CountDownLatch latch = new CountDownLatch(partitions.size());

		for (int partitionIdx = 0; partitionIdx < partitions.size(); partitionIdx++) {
			final int pIdx = partitionIdx;
			final List<T> partition = partitions.get(partitionIdx);

			executor.submit(() -> {
				try {
					processPartition(partition, pIdx, operation, successCount, failureCount, errors);
				} finally {
					latch.countDown();
				}
			});
		}

		// Wait for all partitions to complete
		try {
			latch.await();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.log(Level.WARNING, "Batch execution interrupted", e);
		}
	}

	/**
	 * Process a single partition.
	 */
	private void processPartition(
			List<T> partition,
			int partitionIdx,
			Consumer<T> operation,
			AtomicInteger successCount,
			AtomicInteger failureCount,
			List<ParallelBatchResult.BatchOperationError> errors
	) {
		for (int itemIdx = 0; itemIdx < partition.size(); itemIdx++) {
			T item = partition.get(itemIdx);
			int globalRowIndex = partitionIdx * partitionSize + itemIdx;

			try {
				operation.accept(item);
				successCount.incrementAndGet();
			} catch (Exception e) {
				failureCount.incrementAndGet();
				errors.add(new ParallelBatchResult.BatchOperationError(
						globalRowIndex,
						item.toString(),
						e.getMessage(),
						e
				));
				LOGGER.log(Level.WARNING, "Error processing batch item at index " + globalRowIndex, e);
			}
		}
	}

	/**
	 * Shutdown the executor.
	 */
	public void shutdown() {
		if (executor != null) {
			executor.shutdown();
		}
	}

	/**
	 * Get the configured degree of parallelism.
	 */
	public int getDegreeOfParallelism() {
		return degreeOfParallelism;
	}

	/**
	 * Get the configured partition size.
	 */
	public int getPartitionSize() {
		return partitionSize;
	}

	/**
	 * Default builder implementation for ParallelBatchExecutor.
	 */
	private static final class DefaultBuilder implements Builder {
		private int degreeOfParallelism = 4;
		private int partitionSize = 100;

		@Override
		public Builder degreeOfParallelism(int degreeOfParallelism) {
			this.degreeOfParallelism = degreeOfParallelism;
			return this;
		}

		@Override
		public Builder partitionSize(int partitionSize) {
			this.partitionSize = partitionSize;
			return this;
		}

		@Override
		public <T> ParallelBatchExecutor<T> build() {
			return new ParallelBatchExecutor<>(degreeOfParallelism, partitionSize, true);
		}
	}
}
