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
package com.holonplatform.datastore.jdbc.spring.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.holonplatform.core.datastore.DataTarget;
import com.holonplatform.datastore.jdbc.JdbcDatastore;
import com.holonplatform.datastore.jdbc.spring.EnableJdbcDatastore;
import com.holonplatform.jdbc.spring.EnableDataSource;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = TestMultiDataContext.Config.class)
public class TestMultiDataContext {

	@Configuration
	@PropertySource("test_multi.properties")
	@EnableTransactionManagement
	protected static class Config {

		@Configuration
		@EnableDataSource(dataContextId = "one")
		@EnableJdbcDatastore(dataContextId = "one")
		static class Config1 {
		}

		@Configuration
		@EnableDataSource(dataContextId = "two")
		@EnableJdbcDatastore(dataContextId = "two")
		static class Config2 {
		}

	}

	@Autowired
	@Qualifier("one")
	private JdbcDatastore datastore1;

	@Autowired
	@Qualifier("two")
	private JdbcDatastore datastore2;

	@Test
	public void testDataContext1() {
		assertNotNull(datastore1);

		long count = datastore1.query().target(DataTarget.named("testm1")).count();
		assertEquals(0, count);
	}

	@Test
	public void testDataContext2() {
		assertNotNull(datastore2);

		long count = datastore2.query().target(DataTarget.named("testm2")).count();
		assertEquals(0, count);
	}

}
