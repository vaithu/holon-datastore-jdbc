/*
 * Copyright 2016-2024 Holon Platform (http://holon-platform.com/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.holonplatform.datastore.jdbc.internal.springdata;

import org.springframework.data.domain.Sort;

/**
 * Utility for handling Spring Data Sort conversions.
 * Provides static methods for Spring Data Sort operations.
 */
public class SortMapper {

    /**
     * Validate that Sort contains valid property names.
     *
     * @param sort Sort to validate
     * @return true if sort is valid (always true; override for custom validation)
     */
    public static boolean validate(Sort sort) {
        return true;
    }

    /**
     * Check if sort is unsorted.
     *
     * @param sort Sort to check
     * @return true if unsorted
     */
    public static boolean isUnsorted(Sort sort) {
        return sort == null || sort.isUnsorted();
    }
}
