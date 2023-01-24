/*
  * Copyright 2015-2018 by floragunn GmbH - All rights reserved
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.dlsfls.legacy.lucene;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class MapUtils {

    public static void deepTraverseMap(final Map<String, Object> map, final Callback cb) {
        deepTraverseMap(map, cb, null);
    }

    private static void deepTraverseMap(final Map<String, Object> map, final Callback cb, final List<String> stack) {
        final List<String> localStack;
        if (stack == null) {
            localStack = new ArrayList<String>(30);
        } else {
            localStack = stack;
        }
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() != null && entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> inner = (Map<String, Object>) entry.getValue();
                localStack.add(entry.getKey());
                deepTraverseMap(inner, cb, localStack);
                if (!localStack.isEmpty()) {
                    localStack.remove(localStack.size() - 1);
                }
            } else {
                cb.call(entry.getKey(), map, Collections.unmodifiableList(localStack));
            }
        }
    }

    public static interface Callback {
        public void call(String key, Map<String, Object> map, List<String> stack);
    }
}
