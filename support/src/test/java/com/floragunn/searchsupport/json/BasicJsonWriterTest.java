package com.floragunn.searchsupport.json;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class BasicJsonWriterTest {
    @Test
    public void simpleStringTest() throws Exception {
        assertEquals("\"a\"", BasicJsonWriter.writeAsString("a"));
    }

    @Test
    public void simpleNumberTest() throws Exception {
        assertEquals("42", BasicJsonWriter.writeAsString(42));
    }

    @Test
    public void simpleBooleanTest() throws Exception {
        assertEquals("true", BasicJsonWriter.writeAsString(true));
    }

    @Test
    public void simpleBooleanTest2() throws Exception {
        assertEquals("false", BasicJsonWriter.writeAsString(false));
    }

    @Test
    public void simpleNullTest() throws Exception {
        assertEquals("null", BasicJsonWriter.writeAsString(null));
    }

    @Test
    public void arrayTest() throws Exception {
        Object value = Arrays.asList("abc", 42, true, null, false);

        assertEquals(value, BasicJsonReader.read(BasicJsonWriter.writeAsString(value)));
    }

    @Test
    public void nestedArrayTest() throws Exception {
        Object value = Arrays.asList("abc", 42, true, null, false, Arrays.asList(1, 2, 3), ImmutableMap.of("x", "u"));

        assertEquals(value, BasicJsonReader.read(BasicJsonWriter.writeAsString(value)));
    }

    @Test
    public void objectTest() throws Exception {
        Object value = ImmutableMap.of("x", "u", "y", 42, "z", true);

        assertEquals(value, BasicJsonReader.read(BasicJsonWriter.writeAsString(value)));
    }

    @Test
    public void nestedObjectTest() throws Exception {
        Object value = ImmutableMap.of("x", "u", "y", 42, "z", true, "a", Arrays.asList(1, 2, 3), "b", ImmutableMap.of("foo", "bar", "bla", true));

        assertEquals(value, BasicJsonReader.read(BasicJsonWriter.writeAsString(value)));
    }

}
