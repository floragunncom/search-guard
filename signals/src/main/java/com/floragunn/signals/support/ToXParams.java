/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.support;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.xcontent.ToXContent;

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
