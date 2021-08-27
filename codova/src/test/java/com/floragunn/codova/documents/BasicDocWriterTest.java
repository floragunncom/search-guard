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

import java.util.Arrays;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class BasicDocWriterTest {
    @Test
    public void simpleStringTest() throws Exception {
        assertEquals("\"a\"", DocWriter.writeAsString("a"));
    }

    @Test
    public void simpleNumberTest() throws Exception {
        assertEquals("42", DocWriter.writeAsString(42));
    }

    @Test
    public void simpleBooleanTest() throws Exception {
        assertEquals("true", DocWriter.writeAsString(true));
    }

    @Test
    public void simpleBooleanTest2() throws Exception {
        assertEquals("false", DocWriter.writeAsString(false));
    }

    @Test
    public void simpleNullTest() throws Exception {
        assertEquals("null", DocWriter.writeAsString(null));
    }

    @Test
    public void arrayTest() throws Exception {
        Object value = Arrays.asList("abc", 42, true, null, false);

        assertEquals(value, DocReader.read(DocWriter.writeAsString(value)));
    }

    @Test
    public void nestedArrayTest() throws Exception {
        Object value = Arrays.asList("abc", 42, true, null, false, Arrays.asList(1, 2, 3), ImmutableMap.of("x", "u"));

        assertEquals(value, DocReader.read(DocWriter.writeAsString(value)));
    }

    @Test
    public void objectTest() throws Exception {
        Object value = ImmutableMap.of("x", "u", "y", 42, "z", true);

        assertEquals(value, DocReader.read(DocWriter.writeAsString(value)));
    }

    @Test
    public void nestedObjectTest() throws Exception {
        Object value = ImmutableMap.of("x", "u", "y", 42, "z", true, "a", Arrays.asList(1, 2, 3), "b", ImmutableMap.of("foo", "bar", "bla", true));

        assertEquals(value, DocReader.read(DocWriter.writeAsString(value)));
    }

}
