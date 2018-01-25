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
package com.holonplatform.datastore.jdbc.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.sql.DataSource;

import org.junit.BeforeClass;
import org.junit.Test;

import com.holonplatform.core.datastore.DataTarget;
import com.holonplatform.core.datastore.Datastore.OperationResult;
import com.holonplatform.core.datastore.Datastore.OperationType;
import com.holonplatform.core.datastore.DefaultWriteOption;
import com.holonplatform.core.property.PathProperty;
import com.holonplatform.core.property.PropertyBox;
import com.holonplatform.datastore.jdbc.JdbcDatastore;
import com.holonplatform.datastore.jdbc.test.data.KeyIs;
import com.holonplatform.jdbc.DataSourceBuilder;

public class TestJdbcDatastoreHSQL extends AbstractJdbcDatastoreTest {

	private static JdbcDatastore datastore;

	@BeforeClass
	public static void initDatastore() {

		final DataSource dataSource = DataSourceBuilder.builder().url("jdbc:hsqldb:mem:datastore").username("sa")
				.withInitScriptResource("hsql/schema.sql").withInitScriptResource("hsql/data.sql").build();

		datastore = JdbcDatastore.builder().dataSource(dataSource).withExpressionResolver(KeyIs.RESOLVER)
				.traceEnabled(true).build();

	}

	@Override
	protected JdbcDatastore getDatastore() {
		return datastore;
	}

	private final static PathProperty<Long> CODE = PathProperty.create("code", long.class);
	private final static PathProperty<String> TEXT = PathProperty.create("text", String.class);

	private final static DataTarget<String> TEST2 = DataTarget.named("test2");

	@Test
	public void testAutoIncrement() {

		inTransaction(() -> {

			PropertyBox box = PropertyBox.builder(CODE, TEXT).set(TEXT, "Auto increment 0").build();
			OperationResult result = getDatastore().save(TEST2, box);

			assertNotNull(result);
			assertEquals(1, result.getAffectedCount());
			assertEquals(OperationType.INSERT, result.getOperationType().orElse(null));
			assertEquals(1, result.getInsertedKeys().size());
			assertEquals(Long.valueOf(0), result.getInsertedKeys().values().iterator().next());
			assertEquals("CODE", result.getInsertedKeys().keySet().iterator().next().getName());

			box = PropertyBox.builder(CODE, TEXT).set(TEXT, "Auto increment 1").build();
			result = getDatastore().save(TEST2, box);

			assertNotNull(result);
			assertEquals(1, result.getAffectedCount());
			assertEquals(OperationType.INSERT, result.getOperationType().orElse(null));
			assertEquals(1, result.getInsertedKeys().size());
			assertEquals(Long.valueOf(1), result.getInsertedKeys().values().iterator().next());
			assertEquals("CODE", result.getInsertedKeys().keySet().iterator().next().getName());

			// bring back ids

			box = PropertyBox.builder(CODE, TEXT).set(TEXT, "Auto increment 2").build();
			result = getDatastore().insert(TEST2, box, DefaultWriteOption.BRING_BACK_GENERATED_IDS);

			assertNotNull(result);
			assertEquals(1, result.getAffectedCount());
			assertEquals(OperationType.INSERT, result.getOperationType().orElse(null));

			assertEquals(1, result.getInsertedKeys().size());
			assertEquals(Long.valueOf(2), box.getValue(CODE));

			box = PropertyBox.builder(CODE, TEXT).set(TEXT, "Auto increment 3").build();
			result = getDatastore().save(TEST2, box, DefaultWriteOption.BRING_BACK_GENERATED_IDS);

			assertNotNull(result);
			assertEquals(1, result.getAffectedCount());
			assertEquals(OperationType.INSERT, result.getOperationType().orElse(null));

			assertEquals(1, result.getInsertedKeys().size());
			assertEquals(Long.valueOf(3), box.getValue(CODE));

			// delete all
			getDatastore().bulkDelete(TEST2).execute();

		});
	}

}
