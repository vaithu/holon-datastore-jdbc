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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;

/**
 * Unit tests for SortMapper utility methods.
 */
public class SortMapperUT {

    @Test
    public void testValidateSort() {
        Sort sort = Sort.by("name").ascending();
        assertTrue(SortMapper.validate(sort));
    }

    @Test
    public void testValidateSortUnsorted() {
        Sort sort = Sort.unsorted();
        assertTrue(SortMapper.validate(sort));
    }

    @Test
    public void testIsUnsortedTrue() {
        Sort sort = Sort.unsorted();
        assertTrue(SortMapper.isUnsorted(sort));
    }

    @Test
    public void testIsUnsortedFalse() {
        Sort sort = Sort.by("name").ascending();
        assertFalse(SortMapper.isUnsorted(sort));
    }

    @Test
    public void testIsUnsortedNull() {
        assertTrue(SortMapper.isUnsorted(null));
    }
}
