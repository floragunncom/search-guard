package com.floragunn.signals.support;

import org.elasticsearch.common.xcontent.ToXContent;

import com.google.common.collect.ImmutableMap;

public class ToXParams {

    public static ToXContent.Params of(Enum<?> key, Object value) {
        return new ToXContent.MapParams(ImmutableMap.of(key.name(), value != null ? String.valueOf(value) : null));
    }

    public static ToXContent.Params of(Enum<?> key, boolean value) {
        return new ToXContent.MapParams(ImmutableMap.of(key.name(), Boolean.toString(value)));
    }

    public static ToXContent.Params of(Enum<?> key1, Object value1, Enum<?> key2, Object value2) {
        return new ToXContent.MapParams(ImmutableMap.of(key1.name(), value1 != null ? String.valueOf(value1) : null, key2.name(),
                value2 != null ? String.valueOf(value2) : null));
    }

    public static ToXContent.Params of(Enum<?> key1, boolean value1, Enum<?> key2, boolean value2) {
        return new ToXContent.MapParams(ImmutableMap.of(key1.name(), Boolean.toString(value1), key2.name(), Boolean.toString(value2)));
    }

    public static ToXContent.Params of(Enum<?> key1, Object value1, Enum<?> key2, Object value2, Enum<?> key3, Object value3) {
        return new ToXContent.MapParams(ImmutableMap.of(key1.name(), value1 != null ? String.valueOf(value1) : null, key2.name(),
                value2 != null ? String.valueOf(value2) : null, key3.name(), value3 != null ? String.valueOf(value3) : null));
    }

    public static ToXContent.Params of(Enum<?> key1, boolean value1, Enum<?> key2, boolean value2, Enum<?> key3, boolean value3) {
        return new ToXContent.MapParams(
                ImmutableMap.of(key1.name(), Boolean.toString(value1), key2.name(), Boolean.toString(value2), key3.name(), Boolean.toString(value3)));
    }

}
