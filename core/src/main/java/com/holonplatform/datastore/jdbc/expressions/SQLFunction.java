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
package com.holonplatform.datastore.jdbc.expressions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.holonplatform.core.Expression;
import com.holonplatform.datastore.jdbc.internal.expressions.DefaultSQLFunction;

/**
 * {@link Expression} which represents a SQL function.
 * 
 * @since 5.0.0
 */
public interface SQLFunction extends Expression, Serializable {

	/**
	 * Serialize the function as SQL
	 * @param arguments Optional function arguments
	 * @return Serialized function
	 * @throws InvalidExpressionException If the function arguments are not valid
	 */
	String serialize(List<String> arguments) throws InvalidExpressionException;

	/**
	 * Serialize the function as SQL
	 * @param arguments Function arguments
	 * @return Serialized function
	 */
	default String serialize(String... arguments) {
		return serialize((arguments != null) ? Arrays.asList(arguments) : Collections.emptyList());
	}

	/**
	 * Create a default {@link SQLFunction}.
	 * @param name Function name (not null)
	 * @param parenthesesIfNoArguments Whether parentheses are required if there are no arguments
	 * @return A new {@link SQLFunction} with given name
	 */
	static SQLFunction create(String name, boolean parenthesesIfNoArguments) {
		return new DefaultSQLFunction(name, parenthesesIfNoArguments);
	}

	/**
	 * Create a default {@link SQLFunction}. With this builder, parentheses are not added if there are no arguments.
	 * @param name Function name (not null)
	 * @return A new {@link SQLFunction} with given name
	 */
	static SQLFunction create(String name) {
		return new DefaultSQLFunction(name, false);
	}

}
