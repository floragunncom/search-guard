/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.codova.documents;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class BasicDocReaderTest {

    @Test
    public void simpleStringTest() throws Exception {
        assertEquals("a", DocReader.json().read("\"a\""));
    }

    @Test
    public void simpleNumberTest() throws Exception {
        assertEquals(42, DocReader.json().read("42"));
    }

    @Test
    public void simpleBooleanTest() throws Exception {
        assertEquals(Boolean.TRUE, DocReader.json().read("true"));
    }

    @Test
    public void simpleBooleanTest2() throws Exception {
        assertEquals(Boolean.FALSE, DocReader.json().read("false"));
    }

    @Test
    public void simpleNullTest() throws Exception {
        assertNull(DocReader.json().read("null"));
    }

    @Test
    public void arrayTest() throws Exception {
        assertEquals(Arrays.asList("abc", 42, true, null, false), DocReader.json().read("[\"abc\", 42, true, null, false]"));
    }

    @Test
    public void nestedArrayTest() throws Exception {
        assertEquals(Arrays.asList("abc", 42, true, null, false, Arrays.asList(1, 2, 3), ImmutableMap.of("x", "u")),
                DocReader.json().read("[\"abc\", 42, true, null, false, [1, 2, 3], {\"x\": \"u\"}]"));
    }

    @Test
    public void objectTest() throws Exception {
        assertEquals(ImmutableMap.of("x", "u", "y", 42, "z", true), DocReader.json().read("{\"x\": \"u\", \"y\": 42, \"z\": true}"));
    }

    @Test
    public void nestedObjectTest() throws Exception {
        assertEquals(ImmutableMap.of("x", "u", "y", 42, "z", true, "a", Arrays.asList(1, 2, 3), "b", ImmutableMap.of("foo", "bar", "bla", true)),
                DocReader.json().read("{\"x\": \"u\", \"y\": 42, \"z\": true, \"a\": [1,2,3], \"b\": {\"foo\": \"bar\", \"bla\": true}}"));
    }

}
