/*
 * Copyright 2021-2022 floragunn GmbH
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface Document<T> {
    Object toBasicObject();

    default String toString(Format format) {
        return DocWriter.format(format).writeAsString(this.toBasicObject());
    }

    default byte[] toBytes(Format format) {
        return DocWriter.format(format).writeAsBytes(this.toBasicObject());
    }

    default String toJsonString() {
        return toString(Format.JSON);
    }

    default String toPrettyJsonString() {
        return DocWriter.format(Format.JSON).pretty().writeAsString(this.toBasicObject());
    }

    default String toYamlString() {
        return toString(Format.YAML);
    }

    default byte[] toSmile() {
        return toBytes(Format.SMILE);
    }

    default DocNode toDocNode() {
        Object basicObject = toBasicObject();

        if (basicObject != null) {
            return DocNode.wrap(basicObject);
        } else {
            return null;
        }
    }

    default Object toDeepBasicObject() {
        return toDeepBasicObject(toBasicObject());
    }

    default Metadata<T> meta() {
        return null;
    }

    @SuppressWarnings("unchecked")
    static <T> Document<T> assertedType(Object object, Class<T> type) {
        return (Document<T>) DocNode.wrap(object);
    }

    static Object toDeepBasicObject(Object object) {
        if (object instanceof Document) {
            return ((Document<?>) object).toDeepBasicObject();
        } else if (object instanceof Collection) {
            List<Object> result = new ArrayList<>(((Collection<?>) object).size());

            for (Object subObject : ((Collection<?>) object)) {
                result.add(toDeepBasicObject(subObject));
            }

            return result;
        } else if (object instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>(((Map<?, ?>) object).size());

            for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                result.put(String.valueOf(entry.getKey()), toDeepBasicObject(entry.getValue()));
            }

            return result;
        } else {
            return object;
        }
    }
}
