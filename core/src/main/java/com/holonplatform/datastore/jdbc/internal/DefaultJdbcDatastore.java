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
package com.holonplatform.datastore.jdbc.internal;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Stack;
import java.util.function.Supplier;

import javax.sql.DataSource;

import com.holonplatform.core.Expression;
import com.holonplatform.core.ExpressionResolver;
import com.holonplatform.core.datastore.DatastoreConfigProperties;
import com.holonplatform.core.datastore.transaction.Transaction.TransactionException;
import com.holonplatform.core.datastore.transaction.TransactionConfiguration;
import com.holonplatform.core.datastore.transaction.TransactionalOperation;
import com.holonplatform.core.exceptions.DataAccessException;
import com.holonplatform.core.internal.Logger;
import com.holonplatform.core.internal.datastore.AbstractDatastore;
import com.holonplatform.core.internal.utils.ClassUtils;
import com.holonplatform.core.internal.utils.ObjectUtils;
import com.holonplatform.datastore.jdbc.JdbcDatastore;
import com.holonplatform.datastore.jdbc.JdbcDialect;
import com.holonplatform.datastore.jdbc.composer.ConnectionOperation;
import com.holonplatform.datastore.jdbc.composer.expression.SQLPrimaryKey;
import com.holonplatform.datastore.jdbc.config.JdbcDatastoreCommodityContext;
import com.holonplatform.datastore.jdbc.config.JdbcDatastoreCommodityFactory;
import com.holonplatform.datastore.jdbc.config.JdbcDatastoreExpressionResolver;
import com.holonplatform.datastore.jdbc.dialect.DB2Dialect;
import com.holonplatform.datastore.jdbc.dialect.DefaultDialect;
import com.holonplatform.datastore.jdbc.dialect.DerbyDialect;
import com.holonplatform.datastore.jdbc.dialect.H2Dialect;
import com.holonplatform.datastore.jdbc.dialect.HANADialect;
import com.holonplatform.datastore.jdbc.dialect.HSQLDialect;
import com.holonplatform.datastore.jdbc.dialect.InformixDialect;
import com.holonplatform.datastore.jdbc.dialect.MariaDBDialect;
import com.holonplatform.datastore.jdbc.dialect.MySQLDialect;
import com.holonplatform.datastore.jdbc.dialect.OracleDialect;
import com.holonplatform.datastore.jdbc.dialect.PostgreSQLDialect;
import com.holonplatform.datastore.jdbc.dialect.SQLServerDialect;
import com.holonplatform.datastore.jdbc.dialect.SQLiteDialect;
import com.holonplatform.datastore.jdbc.expressions.SQLParameterDefinition;
import com.holonplatform.datastore.jdbc.internal.context.DefaultPreparedSql;
import com.holonplatform.datastore.jdbc.internal.context.DefaultSQLStatementConfigurator;
import com.holonplatform.datastore.jdbc.internal.context.PreparedSql;
import com.holonplatform.datastore.jdbc.internal.context.SQLStatementConfigurator;
import com.holonplatform.datastore.jdbc.internal.context.StatementConfigurationException;
import com.holonplatform.datastore.jdbc.internal.expressions.JdbcResolutionContext;
import com.holonplatform.datastore.jdbc.internal.operations.JdbcBulkDelete;
import com.holonplatform.datastore.jdbc.internal.operations.JdbcBulkInsert;
import com.holonplatform.datastore.jdbc.internal.operations.JdbcBulkUpdate;
import com.holonplatform.datastore.jdbc.internal.operations.JdbcDelete;
import com.holonplatform.datastore.jdbc.internal.operations.JdbcInsert;
import com.holonplatform.datastore.jdbc.internal.operations.JdbcQuery;
import com.holonplatform.datastore.jdbc.internal.operations.JdbcRefresh;
import com.holonplatform.datastore.jdbc.internal.operations.JdbcSave;
import com.holonplatform.datastore.jdbc.internal.operations.JdbcUpdate;
import com.holonplatform.datastore.jdbc.internal.resolvers.BeanProjectionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.BulkDeleteResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.BulkInsertResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.BulkUpdateResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.CollectionExpressionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.ConstantExpressionProjectionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.ConstantExpressionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.CountAllExpressionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.CountAllProjectionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.DataTargetResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.DefaultQueryFunctionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.DialectQueryFunctionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.ExistFilterResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.LiteralValueResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.NotExistFilterResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.NullExpressionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.OrderBySortResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.PathExpressionProjectionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.PathResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.PrimaryKeyResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.PropertySetProjectionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.QueryAggregationResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.QueryFilterResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.QueryFunctionProjectionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.QueryFunctionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.QueryProjectionResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.QuerySortResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.QueryStructureResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.RelationalTargetResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.SubQueryResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.VisitableQueryFilterResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.VisitableQuerySortResolver;
import com.holonplatform.datastore.jdbc.internal.resolvers.WhereFilterResolver;
import com.holonplatform.datastore.jdbc.internal.transaction.JdbcTransaction;
import com.holonplatform.datastore.jdbc.internal.transaction.JdbcTransactionProvider;
import com.holonplatform.jdbc.DataSourceBuilder;
import com.holonplatform.jdbc.DataSourceConfigProperties;
import com.holonplatform.jdbc.DatabasePlatform;
import com.holonplatform.jdbc.JdbcConnectionHandler;
import com.holonplatform.jdbc.JdbcConnectionHandler.ConnectionType;

/**
 * Default {@link JdbcDatastore} implementation.
 *
 * @since 5.0.0
 */
public class DefaultJdbcDatastore extends AbstractDatastore<JdbcDatastoreCommodityContext>
		implements JdbcDatastoreCommodityContext {

	private static final long serialVersionUID = -1701596812043351551L;

	/**
	 * Logger
	 */
	protected static final Logger LOGGER = JdbcDatastoreLogger.create();

	/**
	 * Current local {@link JdbcTransaction}
	 */
	private static final ThreadLocal<Stack<JdbcTransaction>> CURRENT_TRANSACTION = ThreadLocal
			.withInitial(() -> new Stack<>());

	/**
	 * Shared connection
	 */
	private static final ThreadLocal<Connection> SHARED_CONNECTION = new ThreadLocal<>();

	/**
	 * Data source configuration
	 */
	private DataSourceConfigProperties configuration;

	/**
	 * DataSource
	 */
	private DataSource dataSource;

	/**
	 * Connection handler
	 */
	private JdbcConnectionHandler connectionHandler = JdbcConnectionHandler.create();

	/**
	 * Transaction provider
	 */
	private JdbcTransactionProvider transactionProvider = JdbcTransactionProvider.getDefault();

	/**
	 * Database
	 */
	private DatabasePlatform database;

	/**
	 * Dialect
	 */
	protected JdbcDialect dialect;

	/**
	 * Whether to auto-initialize the Datastore at DataSource/Dialect setup
	 */
	private final boolean autoInitialize;

	/**
	 * Whether the datastore was initialized
	 */
	private boolean initialized = false;

	/**
	 * Max primary keys cache size
	 */
	private final static int MAX_PRIMARY_KEY_CACHE_SIZE = 5000;

	/**
	 * Primary keys cache
	 */
	@SuppressWarnings("serial")
	private final LinkedHashMap<String, SQLPrimaryKey> primaryKeysCache = new LinkedHashMap<String, SQLPrimaryKey>(16,
			0.75f, true) {

		@Override
		protected boolean removeEldestEntry(Entry<String, SQLPrimaryKey> eldest) {
			return size() > MAX_PRIMARY_KEY_CACHE_SIZE;
		}

	};

	/**
	 * Constructor with auto initialization.
	 */
	public DefaultJdbcDatastore() {
		this(true);
	}

	/**
	 * Constructor.
	 * @param autoInitialize Whether to initialize the Datastore at DataSource/Dialect setup
	 */
	public DefaultJdbcDatastore(boolean autoInitialize) {
		super(JdbcDatastoreCommodityFactory.class, JdbcDatastoreExpressionResolver.class);
		this.autoInitialize = autoInitialize;

		// register default resolvers
		addExpressionResolver(new PrimaryKeyResolver(this, primaryKeysCache));
		addExpressionResolver(RelationalTargetResolver.INSTANCE);
		addExpressionResolver(DataTargetResolver.INSTANCE);
		addExpressionResolver(PathResolver.INSTANCE);
		addExpressionResolver(NullExpressionResolver.INSTANCE);
		addExpressionResolver(ConstantExpressionResolver.INSTANCE);
		addExpressionResolver(CollectionExpressionResolver.INSTANCE);
		addExpressionResolver(LiteralValueResolver.INSTANCE);
		addExpressionResolver(CountAllExpressionResolver.INSTANCE);
		addExpressionResolver(QueryFunctionResolver.INSTANCE);
		addExpressionResolver(DialectQueryFunctionResolver.INSTANCE);
		addExpressionResolver(DefaultQueryFunctionResolver.INSTANCE);
		addExpressionResolver(SubQueryResolver.INSTANCE);
		addExpressionResolver(ExistFilterResolver.INSTANCE);
		addExpressionResolver(NotExistFilterResolver.INSTANCE);
		addExpressionResolver(WhereFilterResolver.INSTANCE);
		addExpressionResolver(OrderBySortResolver.INSTANCE);
		addExpressionResolver(VisitableQueryFilterResolver.INSTANCE);
		addExpressionResolver(VisitableQuerySortResolver.INSTANCE);
		addExpressionResolver(QueryFilterResolver.INSTANCE);
		addExpressionResolver(QuerySortResolver.INSTANCE);
		addExpressionResolver(ConstantExpressionProjectionResolver.INSTANCE);
		addExpressionResolver(PathExpressionProjectionResolver.INSTANCE);
		addExpressionResolver(QueryFunctionProjectionResolver.INSTANCE);
		addExpressionResolver(PropertySetProjectionResolver.INSTANCE);
		addExpressionResolver(BeanProjectionResolver.INSTANCE);
		addExpressionResolver(CountAllProjectionResolver.INSTANCE);
		addExpressionResolver(QueryProjectionResolver.INSTANCE);
		addExpressionResolver(QueryAggregationResolver.INSTANCE);
		addExpressionResolver(QueryStructureResolver.INSTANCE);
		addExpressionResolver(BulkInsertResolver.INSTANCE);
		addExpressionResolver(BulkUpdateResolver.INSTANCE);
		addExpressionResolver(BulkDeleteResolver.INSTANCE);

		// register operation commodities
		registerCommodity(JdbcRefresh.FACTORY);
		registerCommodity(JdbcInsert.FACTORY);
		registerCommodity(JdbcUpdate.FACTORY);
		registerCommodity(JdbcSave.FACTORY);
		registerCommodity(JdbcDelete.FACTORY);
		registerCommodity(JdbcBulkInsert.FACTORY);
		registerCommodity(JdbcBulkUpdate.FACTORY);
		registerCommodity(JdbcBulkDelete.FACTORY);
		registerCommodity(JdbcQuery.FACTORY);
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.core.internal.datastore.AbstractDatastore#getCommodityContext()
	 */
	@Override
	protected JdbcDatastoreCommodityContext getCommodityContext() throws CommodityConfigurationException {
		return this;
	}

	/**
	 * Whether to initialize the Datastore at DataSource/dialect setup.
	 * @return the autoInitialize <code>true</code> if auto-initialize is enabled
	 */
	protected boolean isAutoInitialize() {
		return autoInitialize;
	}

	/**
	 * Initialize the datastore if it is not already initialized.
	 * @param classLoader ClassLoader to use to load default factories and resolvers
	 */
	public void initialize(ClassLoader classLoader) {
		if (!initialized) {

			// auto detect platform if not setted
			if (getDatabase().orElse(DatabasePlatform.NONE) == DatabasePlatform.NONE) {
				// get from metadata
				DatabasePlatform platform = withConnection(c -> {
					try {
						DatabaseMetaData dbmd = c.getMetaData();
						if (dbmd != null) {
							String url = dbmd.getURL();
							if (url != null) {
								return DatabasePlatform.fromUrl(url);
							}
						}
					} catch (Exception e) {
						LOGGER.warn("Failed to inspect database metadata", e);
					}
					return null;
				});
				if (platform != DatabasePlatform.NONE) {
					setDatabase(platform);
				}
			}

			// init dialect
			final JdbcDialect dialect = getDialect(false);
			LOGGER.debug(() -> "Datastore JDBC dialect: [" + ((dialect != null) ? dialect.getClass().getName() : null)
					+ "]");
			try {
				dialect.init(this);
			} catch (SQLException e) {
				throw new IllegalStateException("Cannot initialize dialect [" + dialect.getClass().getName() + "]", e);
			}

			// default factories and resolvers
			loadExpressionResolvers(classLoader);
			loadCommodityFactories(classLoader);

			initialized = true;
		}
	}

	/**
	 * Checks whether to auto-initialize the Datastore, if {@link #isAutoInitialize()} is <code>true</code> and the
	 * Datastore wasn't already initialized.
	 */
	protected void checkInitialize() {
		if (isAutoInitialize()) {
			initialize(ClassUtils.getDefaultClassLoader());
		}
	}

	/**
	 * Get the {@link JdbcTransactionProvider} to use to create a new JDBC transaction.
	 * @return the transaction provider
	 */
	protected JdbcTransactionProvider getTransactionProvider() {
		return transactionProvider;
	}

	/**
	 * Set the {@link JdbcTransactionProvider} to use to create a new JDBC transaction.
	 * @param transactionProvider the transaction provider to set (not null)
	 */
	public void setTransactionProvider(JdbcTransactionProvider transactionProvider) {
		ObjectUtils.argumentNotNull(transactionProvider, "JdbcTransactionProvider must be not null");
		this.transactionProvider = transactionProvider;
	}

	/**
	 * Get the data source configuration properties.
	 * @return Optional data source configuration properties
	 */
	public Optional<DataSourceConfigProperties> getConfiguration() {
		return Optional.ofNullable(configuration);
	}

	/**
	 * Set the data source configuration properties.
	 * @param configuration the data source configuration properties to set (not null)
	 * @param buildDataSource Whether to build and set a {@link DataSource} using given configuration properties.
	 */
	public void setConfiguration(DataSourceConfigProperties configuration, boolean buildDataSource) {
		ObjectUtils.argumentNotNull(configuration, "DataSource configuration must be not null");
		this.configuration = configuration;
		if (!getDatabase().isPresent()) {
			setDatabase(configuration.getDatabasePlatform());
		}
		if (buildDataSource) {
			setDataSource(DataSourceBuilder.create().build(configuration));
		}
	}

	/**
	 * Set the {@link DataSource} to be used by this datastore to perform database operations.
	 * @param dataSource the {@link DataSource} to set (not null)
	 */
	public void setDataSource(DataSource dataSource) {
		ObjectUtils.argumentNotNull(dataSource, "DataSource must be not null");
		this.dataSource = dataSource;
		this.primaryKeysCache.clear();
		// initialization
		checkInitialize();
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.datastore.jdbc.config.JdbcDatastoreCommodityContext#getDataSource()
	 */
	@Override
	public DataSource getDataSource() {
		return dataSource;
	}

	/**
	 * Get the {@link JdbcConnectionHandler} which is used for Datastore JDBC connections handling.
	 * @return the connection handler
	 */
	protected JdbcConnectionHandler getConnectionHandler() {
		return connectionHandler;
	}

	/**
	 * Set the {@link JdbcConnectionHandler} to be used for Datastore JDBC connections handling.
	 * @param connectionHandler The connection handler to set (not null)
	 */
	public void setConnectionHandler(JdbcConnectionHandler connectionHandler) {
		ObjectUtils.argumentNotNull(connectionHandler, "JdbcConnectionHandler must be not null");
		this.connectionHandler = connectionHandler;
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.datastore.jdbc.internal.ConfigurableJdbcDatastore#getDatabase()
	 */
	@Override
	public Optional<DatabasePlatform> getDatabase() {
		return Optional.ofNullable(database);
	}

	/**
	 * Set the database platform
	 * @param database the database platform to set
	 */
	public void setDatabase(DatabasePlatform database) {
		this.database = database;
		LOGGER.debug(() -> "Set database platform [" + database.name() + "]");
		// try to setup a suitable dialect
		if (dialect == null) {
			getDialectForPlatform(database).ifPresent(d -> setDialect(d));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.datastore.jdbc.internal.ConfigurableJdbcDatastore#getDialect()
	 */
	@Override
	public JdbcDialect getDialect() {
		return getDialect(true);
	}

	/**
	 * Set the SQL dialect
	 * @param dialect the dialect to set
	 */
	public void setDialect(JdbcDialect dialect) {
		this.dialect = dialect;
		this.primaryKeysCache.clear();

		if (dialect != null) {
			LOGGER.debug(() -> "Set dialect [" + dialect.getClass().getName() + "]");
		}

		// check init
		if (dataSource != null) {
			checkInitialize();
		}
	}

	/**
	 * Get the JDBC dialect of the datastore.
	 * @param checkInitialize <code>true</code> to check if datastore is initialized
	 * @return Datastore dialect
	 */
	protected JdbcDialect getDialect(boolean checkInitialize) {
		if (dialect == null) {
			dialect = new DefaultDialect();
		}

		if (checkInitialize) {
			checkInitialize();
		}

		return dialect;
	}

	/**
	 * Execute given <code>operation</code> with a JDBC {@link Connection} handled by current
	 * {@link JdbcConnectionHandler} and return the operation result.
	 * @param connectionType The connection type (not null)
	 * @param operation The operation to execute (not null)
	 * @return Operation result
	 */
	@SuppressWarnings("resource")
	protected <R> R withConnection(ConnectionType connectionType, ConnectionOperation<R> operation) {
		ObjectUtils.argumentNotNull(operation, "Operation must be not null");

		Connection connection = null;
		try {
			// check shared connection
			if (SHARED_CONNECTION.get() != null) {
				return operation.execute(SHARED_CONNECTION.get());
			}

			// if a transaction is active, use current transaction connection
			JdbcTransaction tx = getCurrentTransaction().orElse(null);
			if (tx != null) {
				return operation.execute(tx.getConnection());
			}

			// get a connection from connection handler
			return operation.execute(connection = obtainConnection(connectionType));

		} catch (Exception e) {
			throw new DataAccessException("Failed to execute operation", e);
		} finally {
			// check active transaction: avoid connection release if present
			if (connection != null) {
				// release connection
				try {
					releaseConnection(connection, connectionType);
				} catch (SQLException e) {
					throw new DataAccessException("Failed to release the connection", e);
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.holonplatform.datastore.jdbc.internal.context.JdbcStatementExecutionContext#withSharedConnection(java.util.
	 * function.Supplier)
	 */
	@Override
	public <R> R withSharedConnection(Supplier<R> operations) {
		ObjectUtils.argumentNotNull(operations, "Operations must be not null");

		if (SHARED_CONNECTION.get() != null) {
			return operations.get();
		}

		return withConnection(connection -> {
			try {
				SHARED_CONNECTION.set(connection);
				return operations.get();
			} finally {
				SHARED_CONNECTION.remove();
			}
		});
	}

	/**
	 * Obtain a new {@link Connection} using current {@link JdbcConnectionHandler}.
	 * @param connectionType Connection type
	 * @return The connection
	 * @throws SQLException If an error occurred
	 */
	private Connection obtainConnection(ConnectionType connectionType) throws SQLException {
		// check DataSource
		final DataSource dataSource = getDataSource();
		if (dataSource == null) {
			throw new IllegalStateException("A DataSource is not available. Check Datastore configuration.");
		}
		// get connection from handler
		Connection connection = getConnectionHandler().getConnection(dataSource, connectionType);
		if (connection == null) {
			throw new IllegalStateException(
					"The connection handler [" + getConnectionHandler() + "] returned a null connection");
		}
		return connection;
	}

	/**
	 * Release (finalize) given {@link Connection} using current {@link JdbcConnectionHandler}.
	 * @param connection The connection to release
	 * @param connectionType Connection type
	 * @throws SQLException If an error occurred
	 */
	private void releaseConnection(Connection connection, ConnectionType connectionType) throws SQLException {
		if (connection != null) {
			getConnectionHandler().releaseConnection(connection, getDataSource(), connectionType);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.holonplatform.datastore.jdbc.JdbcDatastore#withConnection(com.holonplatform.datastore.jdbc.JdbcDatastore.
	 * ConnectionOperation)
	 */
	@Override
	public <R> R withConnection(ConnectionOperation<R> operation) {
		return withConnection(ConnectionType.DEFAULT, operation);
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.datastore.jdbc.JdbcDatastore#withTransaction(com.holonplatform.core.datastore.transaction.
	 * TransactionalOperation, com.holonplatform.core.datastore.transaction.TransactionConfiguration)
	 */
	@Override
	public <R> R withTransaction(TransactionalOperation<R> operation,
			TransactionConfiguration transactionConfiguration) {
		ObjectUtils.argumentNotNull(operation, "TransactionalOperation must be not null");

		final JdbcTransaction tx = beginTransaction(transactionConfiguration);

		try {
			// execute operation
			return operation.execute(tx);
		} catch (Exception e) {
			// check rollback transaction
			if (tx.getConfiguration().isRollbackOnError()) {
				tx.setRollbackOnly();
			}
			throw new DataAccessException("Failed to execute operation", e);
		} finally {
			try {
				finalizeTransaction();
			} catch (Exception e) {
				throw new DataAccessException("Failed to finalize transaction", e);
			}
		}

	}

	/**
	 * Get the current transaction, if active.
	 * @return Optional current transaction
	 */
	private static Optional<JdbcTransaction> getCurrentTransaction() {
		return (CURRENT_TRANSACTION.get().isEmpty()) ? Optional.empty() : Optional.of(CURRENT_TRANSACTION.get().peek());
	}

	/**
	 * If a transaction is active, remove the transaction from current trasactions stack and return the transaction
	 * itself.
	 * @return The removed current transaction, if it was present
	 */
	private static Optional<JdbcTransaction> removeCurrentTransaction() {
		return (CURRENT_TRANSACTION.get().isEmpty()) ? Optional.empty() : Optional.of(CURRENT_TRANSACTION.get().pop());
	}

	/**
	 * Start a new transaction.
	 * @param configuration Transaction configuration
	 * @return The current transaction or a new one if no transaction is active
	 * @throws TransactionException Error starting a new transaction
	 */
	private JdbcTransaction beginTransaction(TransactionConfiguration configuration) throws TransactionException {
		try {
			// create a new transaction
			JdbcTransaction tx = createTransaction(obtainConnection(ConnectionType.DEFAULT),
					(configuration != null) ? configuration : TransactionConfiguration.getDefault());
			// start transaction
			tx.start();
			// stack transaction
			return CURRENT_TRANSACTION.get().push(tx);
		} catch (Exception e) {
			throw new TransactionException("Failed to start a transaction", e);
		}
	}

	/**
	 * Build a new {@link JdbcTransaction} using current {@link JdbcTransactionProvider}.
	 * @param connection The connection to use (not null)
	 * @param configuration Configuration (not null)
	 * @return A new {@link JdbcTransaction}
	 * @throws TransactionException If an error occurred
	 */
	protected JdbcTransaction createTransaction(Connection connection, TransactionConfiguration configuration) {
		return getTransactionProvider().createTransaction(connection, configuration);
	}

	/**
	 * Finalize current transaction, if present.
	 * @return <code>true</code> if a transaction was active and has been finalized
	 * @throws TransactionException Error during transaction finalization
	 */
	private boolean finalizeTransaction() throws TransactionException {
		return removeCurrentTransaction().map(tx -> {
			try {
				// finalize transaction
				tx.end();
				return true;
			} catch (Exception e) {
				throw new TransactionException("Failed to finalize transaction", e);
			} finally {
				// close connection
				try {
					releaseConnection(tx.getConnection(), ConnectionType.DEFAULT);
				} catch (SQLException e) {
					throw new TransactionException("Failed to release the connection", e);
				}
			}
		}).orElse(false);
	}

	// ------- Execution context

	@Override
	public SQLStatementConfigurator<PreparedStatement> getStatementConfigurator() {
		return DefaultSQLStatementConfigurator.INSTANCE;
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.datastore.jdbc.internal.context.StatementExecutionContext#prepareSql(java.lang.String,
	 * com.holonplatform.datastore.jdbc.internal.expressions.JdbcResolutionContext)
	 */
	@Override
	public PreparedSql prepareSql(String sql, JdbcResolutionContext context) {
		ObjectUtils.argumentNotNull(sql, "SQL to prepare must be not null");
		ObjectUtils.argumentNotNull(context, "Resolution context must be not null");
		final Map<String, SQLParameterDefinition> namedParameters = context.getNamedParameters();

		if (!namedParameters.isEmpty()) {

			char[] chars = sql.toCharArray();
			final int length = chars.length;

			StringBuilder sb = new StringBuilder();

			List<SQLParameterDefinition> parameters = new ArrayList<>(namedParameters.size());

			for (int i = 0; i < length; i++) {
				if (chars[i] == ':' && (length - i) >= 7) {
					String namedParameter = String.valueOf(Arrays.copyOfRange(chars, i, i + 7));
					if (namedParameters.containsKey(namedParameter)) {
						sb.append('?');
						parameters.add(namedParameters.get(namedParameter));
						i = i + 6;
						continue;
					}
				}
				sb.append(chars[i]);
			}

			return new DefaultPreparedSql(sb.toString(), parameters);

		}

		return new DefaultPreparedSql(sql, Collections.emptyList());
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.datastore.jdbc.internal.context.JdbcStatementExecutionContext#createStatement(java.sql.
	 * Connection, java.lang.String, java.util.List)
	 */
	@Override
	public PreparedStatement createStatement(Connection connection, PreparedSql sql) throws SQLException {
		ObjectUtils.argumentNotNull(connection, "Connection must be not null");
		ObjectUtils.argumentNotNull(sql, "SQL must be not null");

		PreparedStatement stmt = connection.prepareStatement(sql.getSql());

		try {
			getStatementConfigurator().configureStatement(stmt, sql.getSql(), sql.getParameters());
		} catch (StatementConfigurationException e) {
			throw new SQLException("Failed to configure statement [" + sql + "]", e);
		}

		return stmt;
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.datastore.jdbc.internal.context.StatementExecutionContext#trace(java.lang.String)
	 */
	@Override
	public void trace(String sql) {
		if (isTraceEnabled()) {
			LOGGER.info("(TRACE) SQL: [" + sql + "]");
		} else {
			LOGGER.debug(() -> "SQL: [" + sql + "]");
		}
	}

	/**
	 * Try to detect a suitable {@link JdbcDialect} to use with given {@link DatabasePlatform}.
	 * @param platform DatabasePlatform
	 * @return The {@link JdbcDialect} to use with given platform, or empty if not available
	 */
	protected Optional<JdbcDialect> getDialectForPlatform(DatabasePlatform platform) {
		JdbcDialect dialect = null;
		if (platform != null) {
			switch (platform) {
			case DB2:
				dialect = new DB2Dialect();
				break;
			case DB2_AS400:
				dialect = new DB2Dialect();
				break;
			case DERBY:
				dialect = new DerbyDialect();
				break;
			case H2:
				dialect = new H2Dialect();
				break;
			case HANA:
				dialect = new HANADialect();
				break;
			case HSQL:
				dialect = new HSQLDialect();
				break;
			case INFORMIX:
				dialect = new InformixDialect();
				break;
			case MARIADB:
				dialect = new MariaDBDialect();
				break;
			case MYSQL:
				dialect = new MySQLDialect();
				break;
			case ORACLE:
				dialect = new OracleDialect();
				break;
			case POSTGRESQL:
				dialect = new PostgreSQLDialect();
				break;
			case SQLITE:
				dialect = new SQLiteDialect();
				break;
			case SQL_SERVER:
				dialect = new SQLServerDialect();
				break;
			case NONE:
			default:
				break;
			}
		}
		return Optional.ofNullable(dialect);
	}

	// Builder

	/**
	 * Base {@link JdbcDatastore} builder.
	 *
	 * @param <D> Concrete JdbcDatastore type
	 * @param <I> Concrete datastore instance
	 */
	public static abstract class AbstractBuilder<D extends JdbcDatastore, I extends DefaultJdbcDatastore>
			implements JdbcDatastore.Builder<D> {

		/**
		 * Datastore instance
		 */
		protected final I datastore;

		public AbstractBuilder(I datastore) {
			super();
			this.datastore = datastore;
		}

		/*
		 * (non-Javadoc)
		 * @see com.holonplatform.core.datastore.Datastore.Builder#dataContextId(java.lang.String)
		 */
		@Override
		public JdbcDatastore.Builder<D> dataContextId(String dataContextId) {
			datastore.setDataContextId(dataContextId);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see com.holonplatform.core.datastore.Datastore.Builder#traceEnabled(boolean)
		 */
		@Override
		public JdbcDatastore.Builder<D> traceEnabled(boolean trace) {
			datastore.setTraceEnabled(trace);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * com.holonplatform.core.ExpressionResolver.ExpressionResolverBuilder#withExpressionResolver(com.holonplatform.
		 * core.ExpressionResolver)
		 */
		@Override
		public <E extends Expression, R extends Expression> JdbcDatastore.Builder<D> withExpressionResolver(
				ExpressionResolver<E, R> expressionResolver) {
			datastore.addExpressionResolver(expressionResolver);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see com.holonplatform.datastore.jdbc.JdbcDatastore.Builder#dataSource(javax.sql.DataSource)
		 */
		@Override
		public JdbcDatastore.Builder<D> dataSource(DataSource dataSource) {
			ObjectUtils.argumentNotNull(dataSource, "DataSource must be not null");
			datastore.setDataSource(dataSource);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see com.holonplatform.datastore.jdbc.JdbcDatastore.Builder#dataSource(com.holonplatform.jdbc.
		 * DataSourceConfigProperties)
		 */
		@Override
		public JdbcDatastore.Builder<D> dataSource(DataSourceConfigProperties configuration) {
			ObjectUtils.argumentNotNull(configuration, "DataSource configuration must be not null");
			datastore.setConfiguration(configuration, datastore.getDataSource() == null);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see com.holonplatform.datastore.jdbc.JdbcDatastore.Builder#database(com.holonplatform.jdbc.DatabasePlatform)
		 */
		@Override
		public JdbcDatastore.Builder<D> database(DatabasePlatform database) {
			ObjectUtils.argumentNotNull(database, "Database platform must be not null");
			datastore.setDatabase(database);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * com.holonplatform.datastore.jdbc.JdbcDatastore.Builder#dialect(com.holonplatform.datastore.jdbc.JdbcDialect)
		 */
		@Override
		public JdbcDatastore.Builder<D> dialect(JdbcDialect dialect) {
			ObjectUtils.argumentNotNull(dialect, "Dialect must be not null");
			datastore.setDialect(dialect);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see com.holonplatform.datastore.jdbc.JdbcDatastore.Builder#dialect(java.lang.String)
		 */
		@Override
		public JdbcDatastore.Builder<D> dialect(String dialectClassName) {
			ObjectUtils.argumentNotNull(dialectClassName, "Dialect class name must be not null");
			try {
				datastore.setDialect((JdbcDialect) Class.forName(dialectClassName).newInstance());
			} catch (Exception e) {
				throw new IllegalArgumentException("Failed to istantiate dialect class [" + dialectClassName + "]", e);
			}
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see com.holonplatform.datastore.jdbc.JdbcDatastore.Builder#autoCommit(boolean)
		 */
		@Deprecated
		@Override
		public JdbcDatastore.Builder<D> autoCommit(boolean autoCommit) {
			// noop
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * com.holonplatform.datastore.jdbc.JdbcDatastore.Builder#connectionHandler(com.holonplatform.datastore.jdbc.
		 * JdbcConnectionHandler)
		 */
		@Override
		public JdbcDatastore.Builder<D> connectionHandler(JdbcConnectionHandler connectionHandler) {
			datastore.setConnectionHandler(connectionHandler);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see com.holonplatform.datastore.jdbc.JdbcDatastore.Builder#configuration(com.holonplatform.core.datastore.
		 * DatastoreConfigProperties)
		 */
		@Override
		public JdbcDatastore.Builder<D> configuration(DatastoreConfigProperties configuration) {
			ObjectUtils.argumentNotNull(configuration, "Datastore configuration must be not null");
			datastore.setTraceEnabled(configuration.isTrace());
			String dialect = configuration.getDialect();
			if (dialect != null) {
				return dialect(dialect);
			}
			return this;
		}

	}

	/**
	 * Default {@link JdbcDatastore} builder.
	 */
	public static class DefaultBuilder extends AbstractBuilder<JdbcDatastore, DefaultJdbcDatastore> {

		/**
		 * Constructor
		 */
		public DefaultBuilder() {
			super(new DefaultJdbcDatastore(false));
		}

		/*
		 * (non-Javadoc)
		 * @see com.holonplatform.core.datastore.Datastore.Builder#build()
		 */
		@Override
		public JdbcDatastore build() {
			datastore.initialize(ClassUtils.getDefaultClassLoader());
			return datastore;
		}

	}

}
