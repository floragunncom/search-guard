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

package com.floragunn.codova.util;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class ValueRewritingMapWrapper<K, V1, V2> extends AbstractMap<K, V2> {

    private final Map<K, V1> sourceMap;
    private final Function<V1, V2> valueMappingFunction;

    public ValueRewritingMapWrapper(Map<K, V1> sourceMap, Function<V1, V2> valueMappingFunction) {
        this.sourceMap = sourceMap;
        this.valueMappingFunction = valueMappingFunction;
    }

    @Override
    public int size() {
        return sourceMap.size();
    }

    @Override
    public boolean containsKey(Object key) {
        return sourceMap.containsKey(key);
    }

    @Override
    public V2 get(Object key) {
        if (sourceMap.containsKey(key)) {
            return valueMappingFunction.apply(sourceMap.get(key));
        } else {
            return null;
        }
    }

    @Override
    public Set<K> keySet() {
        return sourceMap.keySet();
    }

    @Override
    public Set<Entry<K, V2>> entrySet() {
        Set<Entry<K, V1>> delegateEntrySet = sourceMap.entrySet();

        return new AbstractSet<Map.Entry<K, V2>>() {

            @Override
            public Iterator<Entry<K, V2>> iterator() {
                Iterator<Entry<K, V1>> delegateIterator = delegateEntrySet.iterator();

                return new Iterator<Map.Entry<K, V2>>() {

                    @Override
                    public boolean hasNext() {
                        return delegateIterator.hasNext();
                    }

                    @Override
                    public Entry<K, V2> next() {
                        Entry<K, V1> sourceEntry = delegateIterator.next();

                        return new AbstractMap.SimpleEntry<>(sourceEntry.getKey(), valueMappingFunction.apply(sourceEntry.getValue()));
                    }
                };
            }

            @Override
            public int size() {
                return delegateEntrySet.size();
            }
        };
    }

};
