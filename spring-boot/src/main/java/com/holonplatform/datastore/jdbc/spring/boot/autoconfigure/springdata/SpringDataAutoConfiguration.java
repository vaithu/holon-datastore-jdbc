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
package com.holonplatform.datastore.jdbc.spring.boot.autoconfigure.springdata;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;

import com.holonplatform.datastore.jdbc.internal.springdata.PageAdapter;
import com.holonplatform.datastore.jdbc.internal.springdata.SliceAdapter;
import com.holonplatform.datastore.jdbc.internal.springdata.SortMapper;

/**
 * Auto-configuration for Spring Data compatibility.
 * Provides PageAdapter, SliceAdapter, and SortMapper beans for paginated queries.
 *
 * Enables:
 * - Spring Data {@link org.springframework.data.domain.Page} support
 * - Spring Data {@link org.springframework.data.domain.Slice} support
 * - Efficient pagination with OFFSET/LIMIT
 * - Lazy loading via Slice (no COUNT query)
 */
@AutoConfiguration
@ConditionalOnClass(name = "org.springframework.data.domain.Pageable")
public class SpringDataAutoConfiguration {

    /**
     * SortMapper bean factory for Spring Data Sort conversion.
     *
     * @return Shared SortMapper utility
     */
    @Bean
    public SortMapper sortMapper() {
        return new SortMapper();
    }
}
