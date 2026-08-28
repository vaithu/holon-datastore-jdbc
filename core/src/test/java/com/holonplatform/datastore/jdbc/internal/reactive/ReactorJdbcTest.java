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

import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.KEY;
import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.NAMED_TARGET;
import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.PROPERTIES;
import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.STR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.holonplatform.core.property.PropertyBox;
import com.holonplatform.datastore.jdbc.test.suite.AbstractJdbcDatastoreSuiteTest;

import reactor.test.StepVerifier;

/**
 * Integration tests for Reactor Flux/Mono integration with JDBC queries.
 *
 * @since 11.2.0
 */
public class ReactorJdbcTest extends AbstractJdbcDatastoreSuiteTest {

	@Test
	void testJdbcFluxFromList() {
		// Create a simple list and wrap with Flux
		var results = getDatastore().query().target(NAMED_TARGET).sort(KEY.asc()).list(PROPERTIES);
		
		// Convert to Flux
		var flux = JdbcFlux.fromIterable(results);
		
		// Verify using StepVerifier
		StepVerifier.create(flux)
			.assertNext(box -> {
				assertEquals(Long.valueOf(1), ((PropertyBox) box).getValue(KEY));
			})
			.assertNext(box -> {
				assertEquals(Long.valueOf(2), ((PropertyBox) box).getValue(KEY));
			})
			.verifyComplete();
	}

	@Test
	void testJdbcFluxFromStream() {
		// Stream results
		var stream = getDatastore().query().target(NAMED_TARGET).sort(KEY.asc()).stream(PROPERTIES);
		
		// Convert to Flux
		var flux = JdbcFlux.fromStream(stream);
		
		// Verify using StepVerifier
		StepVerifier.create(flux)
			.assertNext(box -> {
				assertEquals(Long.valueOf(1), ((PropertyBox) box).getValue(KEY));
			})
			.assertNext(box -> {
				assertEquals(Long.valueOf(2), ((PropertyBox) box).getValue(KEY));
			})
			.verifyComplete();
	}

	@Test
	void testJdbcFluxAsyncQuery() {
		// Async query execution wrapped in Flux
		var query = getDatastore().query().target(NAMED_TARGET).sort(KEY.asc());
		var flux = JdbcFlux.from(query, PROPERTIES);
		
		// Verify with StepVerifier
		StepVerifier.create(flux)
			.assertNext(box -> assertNotNull(box))
			.assertNext(box -> assertNotNull(box))
			.verifyComplete();
	}

	@Test
	void testJdbcFluxAsyncQueryFiltered() {
		// Async query with filter
		var query = getDatastore().query()
			.target(NAMED_TARGET)
			.filter(KEY.eq(1L));
		var flux = JdbcFlux.from(query, PROPERTIES);
		
		// Verify single result
		StepVerifier.create(flux)
			.assertNext(box -> {
				var propertyBox = (PropertyBox) box;
				assertEquals(Long.valueOf(1), propertyBox.getValue(KEY));
				assertEquals("One", propertyBox.getValue(STR));
			})
			.verifyComplete();
	}

	@Test
	void testJdbcFluxWithBackpressure() {
		// Test back-pressure handling with request count
		var query = getDatastore().query().target(NAMED_TARGET).sort(KEY.asc());
		var flux = JdbcFlux.from(query, PROPERTIES);
		
		// Request 1 item at a time
		StepVerifier.create(flux, 1)
			.expectNextCount(1)
			.thenRequest(1)
			.expectNextCount(1)
			.verifyComplete();
	}

	@Test
	void testJdbcMonoFindFirst() {
		// Mono with first result
		var query = getDatastore().query()
			.target(NAMED_TARGET)
			.filter(KEY.eq(1L));
		var mono = JdbcMono.from(query, PROPERTIES);
		
		// Verify result
		StepVerifier.create(mono)
			.assertNext(box -> {
				assertNotNull(box);
				var propertyBox = (PropertyBox) box;
				assertEquals(Long.valueOf(1), propertyBox.getValue(KEY));
			})
			.verifyComplete();
	}

	@Test
	void testJdbcMonoEmpty() {
		// Mono with no results
		var query = getDatastore().query()
			.target(NAMED_TARGET)
			.filter(KEY.eq(999L));
		var mono = JdbcMono.from(query, PROPERTIES);
		
		// Verify empty
		StepVerifier.create(mono)
			.verifyComplete();
	}

	@Test
	void testJdbcMonoCount() {
		// Count query
		var query = getDatastore().query().target(NAMED_TARGET);
		var mono = JdbcMono.count(query);
		
		// Verify count
		StepVerifier.create(mono)
			.assertNext(count -> {
				assertNotNull(count);
				assertEquals(2L, count);
			})
			.verifyComplete();
	}

	@Test
	void testJdbcFluxMap() {
		// Test Flux transformation
		var query = getDatastore().query().target(NAMED_TARGET).sort(KEY.asc());
		var flux = JdbcFlux.from(query, PROPERTIES)
			.map(box -> ((PropertyBox) box).getValue(STR));
		
		// Verify transformed results
		StepVerifier.create(flux)
			.assertNext(name -> assertEquals("One", name))
			.assertNext(name -> assertEquals("Two", name))
			.verifyComplete();
	}

	@Test
	void testJdbcFluxFilter() {
		// Test Flux filtering
		var query = getDatastore().query().target(NAMED_TARGET);
		var flux = JdbcFlux.from(query, PROPERTIES)
			.filter(box -> {
				long key = ((PropertyBox) box).getValue(KEY);
				return key == 1L;
			});
		
		// Verify single filtered result
		StepVerifier.create(flux)
			.assertNext(box -> {
				var propertyBox = (PropertyBox) box;
				assertEquals(Long.valueOf(1), propertyBox.getValue(KEY));
			})
			.verifyComplete();
	}

	@Test
	void testJdbcMonoFromCompletableFuture() {
		// Convert CompletableFuture to Mono
		var asyncQuery = new com.holonplatform.datastore.jdbc.internal.util.AsyncQuery(
			getDatastore().query().target(NAMED_TARGET).filter(KEY.eq(1L))
		);
		
		var mono = JdbcMono.fromFuture(asyncQuery.findOneAsync(PROPERTIES))
			.flatMap(opt -> opt.map(reactor.core.publisher.Mono::just)
				.orElseGet(reactor.core.publisher.Mono::empty));
		
		// Verify
		StepVerifier.create(mono)
			.assertNext(box -> {
				assertNotNull(box);
				assertEquals(Long.valueOf(1), ((PropertyBox) box).getValue(KEY));
			})
			.verifyComplete();
	}

	@Test
	void testJdbcFluxCollect() {
		// Test collecting Flux results
		var query = getDatastore().query().target(NAMED_TARGET).sort(KEY.asc());
		var flux = JdbcFlux.from(query, PROPERTIES);
		
		// Collect to list
		var listMono = flux.collectList();
		
		StepVerifier.create(listMono)
			.assertNext(list -> {
				assertEquals(2, list.size());
				assertEquals(Long.valueOf(1), ((PropertyBox) list.get(0)).getValue(KEY));
				assertEquals(Long.valueOf(2), ((PropertyBox) list.get(1)).getValue(KEY));
			})
			.verifyComplete();
	}

}
