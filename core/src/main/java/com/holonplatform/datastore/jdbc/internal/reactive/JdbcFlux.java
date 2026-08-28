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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import com.holonplatform.core.query.Query;
import com.holonplatform.datastore.jdbc.internal.util.AsyncQuery;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * Reactor Flux adapter for JDBC query execution.
 * <p>
 * Provides stream-oriented, back-pressure-aware query results using Project Reactor's Flux.
 * Queries execute asynchronously on Schedulers.boundedElastic() for optimal I/O handling.
 * </p>
 *
 * <pre>
 * // Example: Query with Flux
 * Query query = datastore.query()
 *     .target(MY_TABLE)
 *     .filter(ACTIVE.eq(true));
 * 
 * JdbcFlux.from(query, MY_PROPERTIES)
 *     .map(box -> box.get(NAME))
 *     .doOnNext(name -> logger.info("Processing: {}", name))
 *     .subscribe(System.out::println);
 * </pre>
 *
 * <pre>
 * // With back-pressure control via StepVerifier
 * StepVerifier.create(
 *     JdbcFlux.from(query, PROPERTIES),
 *     1 // Request 1 item at a time
 * )
 * .expectNextCount(1)
 * .thenRequest(1)
 * .expectNextCount(1)
 * .verifyComplete();
 * </pre>
 *
 * @since 11.2.0
 */
public final class JdbcFlux {

	private JdbcFlux() {
		// utility class
	}

	/**
	 * Create Flux from collection of items.
	 * <p>
	 * Useful for converting eager List results to reactive streams.
	 * </p>
	 *
	 * @param <T> Item type
	 * @param items Collection of items to emit
	 * @return Flux emitting all items
	 */
	public static <T> Flux<T> fromIterable(Collection<T> items) {
		Objects.requireNonNull(items, "Items must not be null");
		return Flux.fromIterable(items);
	}

	/**
	 * Create Flux from Stream of items.
	 * <p>
	 * Stream is consumed and converted to Flux. Useful for wrapping lazy streams
	 * as reactive publishers.
	 * </p>
	 *
	 * @param <T> Item type
	 * @param stream Stream of items to emit
	 * @return Flux emitting all items from stream
	 */
	public static <T> Flux<T> fromStream(Stream<T> stream) {
		Objects.requireNonNull(stream, "Stream must not be null");
		return Flux.fromStream(stream);
	}

	/**
	 * Execute query asynchronously and emit results as Flux.
	 * <p>
	 * Query is executed on virtual thread executor. Results are streamed with
	 * back-pressure support. Execution is deferred until subscription.
	 * </p>
	 *
	 * @param query Query to execute (not null)
	 * @param properties Property iterable to retrieve
	 * @return Flux emitting query results
	 */
	public static Flux<?> from(Query query, Iterable<?> properties) {
		Objects.requireNonNull(query, "Query must not be null");
		Objects.requireNonNull(properties, "Properties must not be null");

		return Flux.defer(() -> {
			// Wrap query with async support
			AsyncQuery asyncQuery = new AsyncQuery(query);
			
			// Execute async list operation
			try {
				var results = asyncQuery.listAsync(properties).get();
				return Flux.fromIterable(results);
			} catch (Exception e) {
				return Flux.error(e);
			}
		})
		.subscribeOn(Schedulers.boundedElastic());
	}

	/**
	 * Execute query asynchronously and emit results as Flux using stream.
	 * <p>
	 * Query is executed on virtual thread executor. Results are emitted as
	 * they arrive from the stream. Offers true lazy evaluation with back-pressure.
	 * </p>
	 *
	 * @param query Query to execute (not null)
	 * @param properties Property iterable to retrieve
	 * @return Flux emitting query results in streaming fashion
	 */
	public static Flux<?> fromStream(Query query, Iterable<?> properties) {
		Objects.requireNonNull(query, "Query must not be null");
		Objects.requireNonNull(properties, "Properties must not be null");

		return Flux.defer(() -> {
			// Wrap query with async support
			AsyncQuery asyncQuery = new AsyncQuery(query);
			
			// Execute async stream operation
			try {
				var results = asyncQuery.streamAsync(properties).get();
				return Flux.fromStream(results);
			} catch (Exception e) {
				return Flux.error(e);
			}
		})
		.subscribeOn(Schedulers.boundedElastic());
	}

}
