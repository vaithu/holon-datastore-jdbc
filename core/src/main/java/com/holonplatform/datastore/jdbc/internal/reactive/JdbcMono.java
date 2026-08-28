/*
 * Copyright 2016-2025 Holonplatform.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.holonplatform.datastore.jdbc.internal.reactive;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import com.holonplatform.core.query.Query;
import com.holonplatform.datastore.jdbc.internal.util.AsyncQuery;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Reactor Mono adapter for JDBC query execution.
 * <p>
 * Provides single-value, non-blocking query results using Project Reactor's Mono.
 * Queries execute asynchronously on Schedulers.boundedElastic() for optimal I/O handling.
 * </p>
 *
 * <pre>
 * // Example: Find first result
 * Query query = datastore.query()
 *     .target(MY_TABLE)
 *     .filter(ID.eq(123L));
 * 
 * JdbcMono.from(query, MY_PROPERTIES)
 *     .map(box -> box.get(NAME))
 *     .doOnNext(name -> logger.info("Found: {}", name))
 *     .subscribe(System.out::println);
 * </pre>
 *
 * <pre>
 * // With error handling
 * JdbcMono.from(query, MY_PROPERTIES)
 *     .onErrorResume(ex -> {
 *         logger.error("Query failed", ex);
 *         return Mono.empty();
 *     })
 *     .block();
 * </pre>
 *
 * @since 11.2.0
 */
public final class JdbcMono {

	private JdbcMono() {
		// utility class
	}

	/**
	 * Convert CompletableFuture to Mono.
	 * <p>
	 * Integrates async CompletableFuture-based results into Reactor's Mono stream.
	 * Useful for wrapping AsyncQuery operations.
	 * </p>
	 *
	 * @param <T> Result type
	 * @param future CompletableFuture to wrap
	 * @return Mono emitting the single result
	 */
	public static <T> Mono<T> fromFuture(CompletableFuture<T> future) {
		return Mono.fromFuture(future);
	}

	/**
	 * Convert Future to Mono.
	 * <p>
	 * Legacy Future interface support. Useful for wrapping older async APIs.
	 * </p>
	 *
	 * @param <T> Result type
	 * @param future Future to wrap
	 * @return Mono emitting the single result
	 */
	public static <T> Mono<T> fromFuture(Future<T> future) {
		if (future instanceof CompletableFuture) {
			return Mono.fromFuture((CompletableFuture<T>) future);
		}
		// Convert Future to CompletableFuture
		return Mono.fromFuture(CompletableFuture.supplyAsync(() -> {
			try {
				return future.get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}));
	}

	/**
	 * Execute query asynchronously and emit first result as Mono.
	 * <p>
	 * Query is executed on virtual thread executor. Returns Optional.empty()
	 * if no results found. Execution is deferred until subscription.
	 * </p>
	 *
	 * @param query Query to execute (not null)
	 * @param properties Property iterable to retrieve
	 * @return Mono emitting optional result
	 */
	public static Mono<?> from(Query query, Iterable<?> properties) {
		java.util.Objects.requireNonNull(query, "Query must not be null");
		java.util.Objects.requireNonNull(properties, "Properties must not be null");

		return Mono.defer(() -> {
			// Wrap query with async support
			AsyncQuery asyncQuery = new AsyncQuery(query);
			
			// Execute async findOne operation and flatten Optional
			return Mono.fromFuture(asyncQuery.findOneAsync(properties))
				.flatMap(opt -> ((Optional<?>) opt).map(Mono::just).orElseGet(Mono::empty));
		})
		.subscribeOn(Schedulers.boundedElastic());
	}

	/**
	 * Execute count query asynchronously and emit result as Mono.
	 * <p>
	 * Query is executed on virtual thread executor. Returns the count as a Long.
	 * Execution is deferred until subscription.
	 * </p>
	 *
	 * @param query Query to execute (not null)
	 * @return Mono emitting count result
	 */
	public static Mono<Long> count(Query query) {
		java.util.Objects.requireNonNull(query, "Query must not be null");

		return Mono.defer(() -> {
			// Wrap query with async support
			AsyncQuery asyncQuery = new AsyncQuery(query);
			
			// Execute async count operation
			return Mono.fromFuture(asyncQuery.countAsync());
		})
		.subscribeOn(Schedulers.boundedElastic());
	}

}
