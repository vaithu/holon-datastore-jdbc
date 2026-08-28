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
package com.holonplatform.datastore.jdbc.test.suite;

import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.KEY;
import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.NAMED_TARGET;
import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.PROPERTIES;
import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.STR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.holonplatform.core.property.PropertyBox;
import com.holonplatform.datastore.jdbc.internal.util.AsyncQuery;

/**
 * Integration tests for AsyncQuery async query execution.
 *
 * @since 11.2.0
 */
class AsyncQueryTest extends AbstractJdbcDatastoreSuiteTest {

	@Test
	void testListAsync() throws Exception {
		var query = getDatastore().query().target(NAMED_TARGET).sort(KEY.asc());
		var asyncQuery = new AsyncQuery(query);
		
		// Execute async list
		var future = asyncQuery.listAsync(PROPERTIES);
		List<PropertyBox> results = future.get(5, TimeUnit.SECONDS);
		
		// Verify results
		assertNotNull(results);
		assertEquals(2, results.size());
		assertEquals(Long.valueOf(1), results.get(0).getValue(KEY));
		assertEquals("One", results.get(0).getValue(STR));
	}

	@Test
	void testFindOneAsync() throws Exception {
		var query = getDatastore().query().target(NAMED_TARGET).filter(KEY.eq(1L));
		var asyncQuery = new AsyncQuery(query);
		
		// Execute async findOne
		var future = asyncQuery.findOneAsync(PROPERTIES);
		Optional<PropertyBox> result = future.get(5, TimeUnit.SECONDS);
		
		// Verify result
		assertTrue(result.isPresent());
		assertEquals(Long.valueOf(1), result.get().getValue(KEY));
		assertEquals("One", result.get().getValue(STR));
	}

	@Test
	void testFindOneAsyncEmpty() throws Exception {
		var query = getDatastore().query().target(NAMED_TARGET).filter(KEY.eq(999L));
		var asyncQuery = new AsyncQuery(query);
		
		// Execute async findOne
		var future = asyncQuery.findOneAsync(PROPERTIES);
		Optional<PropertyBox> result = future.get(5, TimeUnit.SECONDS);
		
		// Verify empty result
		assertFalse(result.isPresent());
	}

	@Test
	void testStreamAsync() throws Exception {
		var query = getDatastore().query().target(NAMED_TARGET).sort(KEY.asc());
		var asyncQuery = new AsyncQuery(query);
		
		// Execute async stream
		var future = asyncQuery.streamAsync(PROPERTIES);
		Stream<PropertyBox> results = future.get(5, TimeUnit.SECONDS);
		
		// Consume stream
		long count = results.count();
		assertEquals(2, count);
	}

	@Test
	void testCountAsync() throws Exception {
		var query = getDatastore().query().target(NAMED_TARGET);
		var asyncQuery = new AsyncQuery(query);
		
		// Execute async count
		var future = asyncQuery.countAsync();
		Long count = future.get(5, TimeUnit.SECONDS);
		
		// Verify count
		assertNotNull(count);
		assertEquals(2L, count);
	}

	@Test
	void testAsyncComposition() throws Exception {
		var query = getDatastore().query().target(NAMED_TARGET);
		var asyncQuery = new AsyncQuery(query);
		
		// Compose multiple async operations
		var future = asyncQuery.countAsync()
			.thenCompose(count -> {
				if (count > 0) {
					return asyncQuery.listAsync(PROPERTIES)
						.thenApply(list -> list.size());
				}
				return java.util.concurrent.CompletableFuture.completedFuture(0);
			});
		
		Integer size = future.get(5, TimeUnit.SECONDS);
		assertEquals(2, size);
	}

	@Test
	void testGetQueryMethod() {
		var query = getDatastore().query().target(NAMED_TARGET);
		var asyncQuery = new AsyncQuery(query);
		
		// Verify getQuery returns wrapped query
		assertNotNull(asyncQuery.getQuery());
		assertEquals(query, asyncQuery.getQuery());
	}

}
