/*
 * Copyright 2016-2018 Axioma srl.
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
package com.holonplatform.datastore.jdbc.test.suite;

import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.KEY;
import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.NAMED_TARGET;
import static com.holonplatform.datastore.jdbc.test.data.TestDataModel.STR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import com.holonplatform.core.query.QueryFunction;

import org.junit.jupiter.api.Test;
import com.holonplatform.core.query.QueryFunction.Count;

class AggregationFunctionsTest extends AbstractJdbcDatastoreSuiteTest {

	@Test
	void testCount() {

		long result = getDatastore().query().target(NAMED_TARGET).findOne(Count.create(STR)).orElse(-1L);
		assertEquals(2, result);

		result = getDatastore().query().target(NAMED_TARGET).findOne(KEY.count()).orElse(-1L);
		assertEquals(2, result);

	}

	@Test
	void testMinMax() {
		Optional<Long> key = getDatastore().query().target(NAMED_TARGET).findOne(KEY.max());
		assertTrue(key.isPresent());
		assertEquals(Long.valueOf(2), key.get());

		key = getDatastore().query().target(NAMED_TARGET).findOne(KEY.min());
		assertTrue(key.isPresent());
		assertEquals(Long.valueOf(1), key.get());
	}

	@Test
	void testSum() {
		Optional<Long> sum = getDatastore().query().target(NAMED_TARGET).findOne(QueryFunction.sum(KEY));
		assertTrue(sum.isPresent());
		assertEquals(Long.valueOf(3), sum.get());
	}

	@Test
	void testAvg() {
		Optional<Double> avg = getDatastore().query().target(NAMED_TARGET).findOne(KEY.avg());
		assertTrue(avg.isPresent());
		assertEquals(Double.valueOf(1.5), avg.get());
	}

}
