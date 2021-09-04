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

import java.util.LinkedHashMap;
import java.util.Map;

public class DocUtils {
    public static Map<String, Object> toStringKeyedMap(Map<?, ?> map) {
        boolean allKeysAreStrings = map.keySet().stream().allMatch((o) -> o instanceof String || o == null);

        if (allKeysAreStrings) {
            @SuppressWarnings("unchecked")
            Map<String, Object> result = (Map<String, Object>) map;
            return result;
        } else {
            Map<String, Object> result = new LinkedHashMap<>(map.size());

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(entry.getKey() != null ? entry.getKey().toString() : null, entry.getValue());
            }

            return result;
        }
    }
}
