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
package com.holonplatform.datastore.jdbc.composer;

import java.sql.SQLException;
import java.util.Optional;

/**
 * TODO
 *
 * @since 5.1.0
 */
public interface SQLResult {

	/**
	 * Get the value at given index.
	 * @param index Result value index, starting from <code>1</code>.
	 * @return The result value
	 * @throws SQLException Error retrieving the result value
	 */
	Object getValue(int index) throws SQLException;

	/**
	 * Get the value with given name.
	 * @param name Resut value name (not null)
	 * @return The result value
	 * @throws SQLException Error retrieving the result value
	 */
	Object getValue(String name) throws SQLException;

	/**
	 * Get the number of available result values.
	 * @return the number of available result values
	 * @throws SQLException If an error occurred
	 */
	int getValueCount() throws SQLException;

	/**
	 * Get the result value name at given index, if available.
	 * @param index Result value index, starting from <code>1</code>.
	 * @return Optional result value name
	 * @throws SQLException If an error occurred
	 */
	Optional<String> getValueName(int index) throws SQLException;

}
