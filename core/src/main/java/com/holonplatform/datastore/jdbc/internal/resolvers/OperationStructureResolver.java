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
package com.holonplatform.datastore.jdbc.internal.resolvers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import com.holonplatform.core.Expression;
import com.holonplatform.core.Expression.InvalidExpressionException;
import com.holonplatform.core.ExpressionResolver;
import com.holonplatform.core.Path;
import com.holonplatform.core.datastore.Datastore.OperationType;
import com.holonplatform.core.datastore.relational.RelationalTarget;
import com.holonplatform.core.property.Property;
import com.holonplatform.core.query.ConstantExpression;
import com.holonplatform.core.query.QueryExpression;
import com.holonplatform.datastore.jdbc.expressions.SQLToken;
import com.holonplatform.datastore.jdbc.internal.expressions.JdbcResolutionContext;
import com.holonplatform.datastore.jdbc.internal.expressions.OperationStructure;

/**
 * {@link OperationStructure} expression resolver.
 *
 * @since 5.0.0
 */
public enum OperationStructureResolver implements ExpressionResolver<OperationStructure, SQLToken> {

	/**
	 * Singleton instance.
	 */
	INSTANCE;

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.core.ExpressionResolver#getExpressionType()
	 */
	@Override
	public Class<? extends OperationStructure> getExpressionType() {
		return OperationStructure.class;
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.core.ExpressionResolver#getResolvedType()
	 */
	@Override
	public Class<? extends SQLToken> getResolvedType() {
		return SQLToken.class;
	}

	/*
	 * (non-Javadoc)
	 * @see com.holonplatform.core.Expression.ExpressionResolverFunction#resolve(com.holonplatform.core.Expression,
	 * com.holonplatform.core.ExpressionResolver.ResolutionContext)
	 */
	@Override
	public Optional<SQLToken> resolve(OperationStructure expression, ResolutionContext resolutionContext)
			throws InvalidExpressionException {

		// validate
		expression.validate();

		// context
		final JdbcResolutionContext context = JdbcResolutionContext.checkContext(resolutionContext);

		// from
		final RelationalTarget<?> target = context.resolveExpression(expression.getTarget(), RelationalTarget.class);

		context.setTarget(target);

		// configure statement

		final StringBuilder operation = new StringBuilder();

		// check type
		final OperationType type = expression.getOperationType();

		switch (type) {
		case DELETE:
			if (context.getDialect().deleteStatementTargetRequired()) {
				operation.append("DELETE");
				context.getAlias(target).ifPresent(a -> {
					operation.append(" ");
					operation.append(a);
				});
				operation.append(" FROM");
			} else {
				operation.append("DELETE FROM");
			}
			break;
		case INSERT:
			operation.append("INSERT INTO");
			break;
		case UPDATE:
			operation.append("UPDATE");
			break;
		default:
			break;
		}

		operation.append(" ");

		// target
		operation.append(context.resolveExpression(target, SQLToken.class).getValue());

		// values
		if (type == OperationType.INSERT || type == OperationType.UPDATE) {

			final Map<Path<?>, QueryExpression<?>> pathValues = expression.getValues();
			final List<String> paths = new ArrayList<>(pathValues.size());
			final List<String> values = new ArrayList<>(pathValues.size());

			// resolve path and value
			for (Entry<Path<?>, QueryExpression<?>> entry : pathValues.entrySet()) {
				paths.add(resolveExpression(entry.getKey(), context));
				values.add(resolvePathValue(entry.getKey(), entry.getValue(), context, true));
			}

			// configure statement
			if (type == OperationType.INSERT) {

				operation.append(" (");
				operation.append(paths.stream().collect(Collectors.joining(",")));
				operation.append(") VALUES (");
				operation.append(values.stream().collect(Collectors.joining(",")));
				operation.append(")");

			} else if (type == OperationType.UPDATE) {

				operation.append(" SET ");
				for (int i = 0; i < paths.size(); i++) {
					if (i > 0) {
						operation.append(",");
					}
					operation.append(paths.get(i));
					operation.append("=");
					operation.append(values.get(i));
				}
			}

		}

		// filter
		if (type != OperationType.INSERT) {
			expression.getFilter().ifPresent(f -> {
				operation.append(" WHERE ");
				operation.append(context.resolveExpression(f, SQLToken.class).getValue());
			});
		}

		// return SQL statement
		return Optional.of(SQLToken.create(operation.toString()));
	}

	/**
	 * Resolve given {@link Expression} to obtain the corresponding SQL expression.
	 * @param expression Expression to resolve
	 * @param context Resolution context
	 * @param clause Resolution clause
	 * @return SQL expression
	 * @throws InvalidExpressionException If expression cannot be resolved
	 */
	private static String resolveExpression(Expression expression, JdbcResolutionContext context) {
		SQLToken token = context.resolve(expression, SQLToken.class, context)
				.orElseThrow(() -> new InvalidExpressionException("Failed to resolve expression [" + expression + "]"));
		token.validate();
		return token.getValue();
	}

	/**
	 * Resolve a value associated to a {@link Path} to obtain the corresponding SQL expression.
	 * @param path Path
	 * @param expression Value expression
	 * @param context Resolution context
	 * @param clause Resolution clause
	 * @param allowNull if <code>true</code>, null values are allowed and returned as <code>NULL</code> keyword
	 * @return SQL expression
	 * @throws InvalidExpressionException If expression cannot be resolved
	 */
	@SuppressWarnings("unchecked")
	private static String resolvePathValue(Path<?> path, QueryExpression<?> expression, JdbcResolutionContext context,
			boolean allowNull) {

		if (expression instanceof ConstantExpression) {
			Object value = ((ConstantExpression<?, ?>) expression).getValue();
			if ("?".equals(value)) {
				return "?";
			}

			// check converter
			if (path instanceof Property) {
				return context.resolveExpression(
						ConstantExpression.create(((Property<Object>) path).getConvertedValue(value)), SQLToken.class)
						.getValue();
			}
		}

		return context.resolveExpression(expression, SQLToken.class).getValue();
	}

}
