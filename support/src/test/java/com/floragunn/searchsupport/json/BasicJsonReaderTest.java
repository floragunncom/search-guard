package com.floragunn.searchsupport.json;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class BasicJsonReaderTest {
    @Test
    public void simpleStringTest() throws Exception {
        assertEquals("a", BasicJsonReader.read("\"a\""));
    }

    @Test
    public void simpleNumberTest() throws Exception {
        assertEquals(42, BasicJsonReader.read("42"));
    }

    @Test
    public void simpleBooleanTest() throws Exception {
        assertEquals(Boolean.TRUE, BasicJsonReader.read("true"));
    }

    @Test
    public void simpleBooleanTest2() throws Exception {
        assertEquals(Boolean.FALSE, BasicJsonReader.read("false"));
    }

    @Test
    public void simpleNullTest() throws Exception {
        assertNull(BasicJsonReader.read("null"));
    }

    @Test
    public void arrayTest() throws Exception {
        assertEquals(Arrays.asList("abc", 42, true, null, false), BasicJsonReader.read("[\"abc\", 42, true, null, false]"));
    }

    @Test
    public void nestedArrayTest() throws Exception {
        assertEquals(Arrays.asList("abc", 42, true, null, false, Arrays.asList(1, 2, 3), ImmutableMap.of("x", "u")),
                BasicJsonReader.read("[\"abc\", 42, true, null, false, [1, 2, 3], {\"x\": \"u\"}]"));
    }

    @Test
    public void objectTest() throws Exception {
        assertEquals(ImmutableMap.of("x", "u", "y", 42, "z", true), BasicJsonReader.read("{\"x\": \"u\", \"y\": 42, \"z\": true}"));
    }

    @Test
    public void nestedObjectTest() throws Exception {
        assertEquals(ImmutableMap.of("x", "u", "y", 42, "z", true, "a", Arrays.asList(1, 2, 3), "b", ImmutableMap.of("foo", "bar", "bla", true)),
                BasicJsonReader.read("{\"x\": \"u\", \"y\": 42, \"z\": true, \"a\": [1,2,3], \"b\": {\"foo\": \"bar\", \"bla\": true}}"));
    }

}
