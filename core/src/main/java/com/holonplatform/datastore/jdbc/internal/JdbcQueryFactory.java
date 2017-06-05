/*
 * Copyright 2000-2016 Holon TDCN.
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
package com.holonplatform.datastore.jdbc.internal;

import com.holonplatform.core.datastore.DatastoreCommodityContext.CommodityConfigurationException;
import com.holonplatform.core.datastore.DatastoreCommodityFactory;
import com.holonplatform.core.internal.query.QueryAdapterQuery;
import com.holonplatform.core.internal.query.QueryDefinition;
import com.holonplatform.core.query.Query;
import com.holonplatform.datastore.jdbc.config.JdbcDatastoreCommodityContext;

/**
 * Default {@link Query} factory.
 * 
 * @since 5.0.0
 */
public class JdbcQueryFactory implements DatastoreCommodityFactory<JdbcDatastoreCommodityContext, Query> {

	private static final long serialVersionUID = -7239034174753828511L;

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.core.datastore.DatastoreCommodityFactory#getCommodityType()
	 */
	@Override
	public Class<? extends Query> getCommodityType() {
		return Query.class;
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.core.datastore.DatastoreCommodityFactory#createCommodity(com.holonplatform.core.datastore.
	 * DatastoreCommodityContext)
	 */
	@Override
	public Query createCommodity(JdbcDatastoreCommodityContext context) throws CommodityConfigurationException {
		return new QueryAdapterQuery<>(context,
				new JdbcQueryAdapter(context, context.getDialect(), context.isTraceEnabled()),
				QueryDefinition.create());
	}

}
