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

package com.floragunn.searchsupport.util;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface ImmutableMap<K, V> extends Map<K, V> {

    public static <K, V> ImmutableMap<K, V> of(Map<K, V> map) {
        if (map instanceof ImmutableMap) {
            return (ImmutableMap<K, V>) map;
        } else if (map.size() == 0) {
            return empty();
        } else if (map.size() == 1) {
            Map.Entry<K, V> entry = map.entrySet().iterator().next();
            return new SingleElementMap<>(entry.getKey(), entry.getValue());
        } else if (map.size() == 2) {
            Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
            Map.Entry<K, V> entry1 = iter.next();
            Map.Entry<K, V> entry2 = iter.next();
            return new TwoElementMap<>(entry1.getKey(), entry1.getValue(), entry2.getKey(), entry2.getValue());
        } else {
            return new MapBackedMap<K, V>(new LinkedHashMap<K, V>(map));
        }
    }

    public static <K, V> ImmutableMap<K, V> of(K k1, V v1) {
        return new SingleElementMap<>(k1, v1);
    }

    public static <K, V> ImmutableMap<K, V> of(K k1, V v1, K k2, V v2) {
        if (k1.equals(k2)) {
            return new SingleElementMap<>(k1, v1);
        } else {
            return new TwoElementMap<>(k1, v1, k2, v2);
        }
    }

    public static <K, V> ImmutableMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
        if (k1.equals(k2)) {
            if (k2.equals(k3)) {
                return new SingleElementMap<>(k1, v1);
            } else {
                return new TwoElementMap<>(k1, v1, k3, v3);
            }
        } else if (k2.equals(k3)) {
            // k1 != k2
            return new TwoElementMap<>(k1, v1, k2, v2);
        } else if (k1.equals(k3)) {
            // k1 != k2
            return new TwoElementMap<>(k1, v1, k2, v2);
        } else {
            return new ArrayBackedMap<>(k1, v1, k2, v2, k3, v3);
        }
    }

    public static <K, V> ImmutableMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        if (k1.equals(k2)) {
            if (k2.equals(k3)) {
                if (k3.equals(k4)) {
                    return new SingleElementMap<>(k1, v1);
                } else {
                    return new TwoElementMap<>(k1, v1, k4, v4);
                }
            } else {
                if (k3.equals(k4)) {
                    return new TwoElementMap<>(k1, v1, k3, v3);
                } else {
                    return new ArrayBackedMap<>(k1, v1, k3, v3, k4, v4);
                }
            }
        } else {
            // k1 != k2

            if (k2.equals(k3)) {
                if (k3.equals(k4)) {
                    return new TwoElementMap<>(k1, v1, k2, v2);
                } else {
                    return new ArrayBackedMap<>(k1, v1, k2, v2, k4, v4);
                }
            } else {
                if (k3.equals(k4)) {
                    return new ArrayBackedMap<>(k1, v1, k2, v2, k3, v3);
                } else {
                    return new ArrayBackedMap<>(k1, v1, k2, v2, k3, v3, k4, v4);
                }
            }
        }
    }

    public static <K, V> ImmutableMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        if (k1.equals(k2)) {
            if (k2.equals(k3)) {
                if (k3.equals(k4)) {
                    if (k4.equals(k5)) {
                        return new SingleElementMap<>(k1, v1);
                    } else {
                        return new TwoElementMap<>(k1, v1, k4, v4);
                    }
                } else {
                    // k3 != k4

                    if (k4.equals(k5)) {
                        return new TwoElementMap<>(k1, v1, k4, v4);
                    } else {
                        return new ArrayBackedMap<>(k1, v1, k4, v4, k5, v5);
                    }
                }
            } else {
                // k2 != k3

                if (k3.equals(k4)) {
                    if (k4.equals(k5)) {
                        return new TwoElementMap<>(k1, v1, k3, v3);
                    } else {
                        return new ArrayBackedMap<>(k1, v1, k3, v3, k5, v5);
                    }
                } else {
                    // k3 != k4

                    if (k4.equals(k5)) {
                        return new ArrayBackedMap<>(k1, v1, k3, v3, k4, v4);
                    } else {
                        return new ArrayBackedMap<>(k1, v1, k3, v3, k4, v4, k5, v5);
                    }
                }
            }
        } else {
            // k1 != k2

            if (k2.equals(k3)) {
                if (k3.equals(k4)) {
                    if (k4.equals(k5)) {
                        return new TwoElementMap<>(k1, v1, k2, v2);
                    } else {
                        return new ArrayBackedMap<>(k1, v1, k2, v2, k4, v4, k5, v5);
                    }
                } else {
                    // k3 != k4
                    if (k4.equals(k5)) {
                        return new ArrayBackedMap<>(k1, v1, k2, v2, k4, v4);
                    } else {
                        return new ArrayBackedMap<>(k1, v1, k2, v2, k4, v4, k5, v5);
                    }
                }
            } else {
                // k2 != k3

                if (k3.equals(k4)) {
                    if (k4.equals(k5)) {
                        return new ArrayBackedMap<>(k1, v1, k2, v2, k3, v3);
                    } else {
                        return new ArrayBackedMap<>(k1, v1, k2, v2, k3, v3, k5, v5);
                    }
                } else {
                    // k3 != k4

                    if (k4.equals(k5)) {
                        return new ArrayBackedMap<>(k1, v1, k2, v2, k3, v3, k4, v4);
                    } else {
                        return new ArrayBackedMap<>(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5);
                    }
                }
            }
        }
    }

    public static <K, V> ImmutableMap<K, V> ofNonNull(K k1, V v1) {
        if (k1 != null && v1 != null) {
            return of(k1, v1);
        } else {
            return empty();
        }
    }

    public static <K, V> ImmutableMap<K, V> ofNonNull(K k1, V v1, K k2, V v2) {
        if (v1 != null && k1 != null && v2 != null && k2 != null) {
            return of(k1, v1, k2, v2);
        } else if (k1 != null && v1 != null) {
            return of(k1, v1);
        } else if (k2 != null && v2 != null) {
            return of(k2, v2);
        } else {
            return empty();
        }
    }

    public static <K, V> ImmutableMap<K, V> ofNonNull(K k1, V v1, K k2, V v2, K k3, V v3) {
        if (k3 == null || v3 == null) {
            return ofNonNull(k1, v1, k2, v2);
        }

        if (k1 != null && v1 != null) {
            if (k2 != null && v2 != null) {
                return of(k1, v1, k2, v2, k3, v3);
            } else {
                return of(k1, v1, k3, v3);
            }
        } else {
            if (k2 != null && v2 != null) {
                return of(k2, v2, k3, v3);
            } else {
                return of(k3, v3);

            }
        }
    }

    public static <K, V> ImmutableMap<K, V> ofNonNull(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {        
        if (k4 == null || v4 == null) {
            return ofNonNull(k1, v1, k2, v2, k3, v3);
        }

        if (k1 != null && v1 != null) {
            if (k2 != null && v2 != null) {
                if (k3 != null && v3 != null) {
                    return of(k1, v1, k2, v2, k3, v3, k4, v4);
                } else {
                    return of(k1, v1, k2, v2, k4, v4);
                }
            } else {
                if (k3 != null && v3 != null) {
                    return of(k1, v1, k3, v3, k4, v4);
                } else {
                    return of(k1, v1, k4, v4);
                }
            }
        } else {
            if (k2 != null && v2 != null) {
                if (k3 != null && v3 != null) {
                    return of(k2, v2, k3, v3, k4, v4);
                } else {
                    return of(k2, v2, k4, v4);
                }
            } else {
                if (k3 != null && v3 != null) {
                    return of(k3, v3, k4, v4);
                } else {
                    return of(k4, v4);
                }
            }
        }
    }

    public static <K, V> ImmutableMap<K, V> ofNonNull(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        if (k5 == null || v5 == null) {
            return ofNonNull(k1, v1, k2, v2, k3, v3, k4, v4);
        }

        if (k1 != null && v1 != null) {
            if (k2 != null && v2 != null) {
                if (k3 != null && v3 != null) {
                    if (k4 != null && v4 != null) {
                        return of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5);
                    } else {
                        return of(k1, v1, k2, v2, k3, v3, k5, v5);
                    }
                } else {
                    if (k4 != null && v4 != null) {
                        return of(k1, v1, k2, v2, k4, v4, k5, v5);
                    } else {
                        return of(k1, v1, k2, v2, k5, v5);
                    }
                }
            } else {
                if (k3 != null && v3 != null) {
                    if (k4 != null && v4 != null) {
                        return of(k1, v1, k3, v3, k4, v4, k5, v5);
                    } else {
                        return of(k1, v1, k3, v3, k5, v5);
                    }
                } else {
                    if (k4 != null && v4 != null) {
                        return of(k1, v1, k4, v4, k5, v5);
                    } else {
                        return of(k1, v1, k5, v5);
                    }
                }
            }
        } else {
            if (k2 != null && v2 != null) {
                if (k3 != null && v3 != null) {
                    if (k4 != null && v4 != null) {
                        return of(k2, v2, k3, v3, k4, v4, k5, v5);
                    } else {
                        return of(k2, v2, k3, v3, k5, v5);
                    }
                } else {
                    if (k4 != null && v4 != null) {
                        return of(k2, v2, k4, v4, k5, v5);
                    } else {
                        return of(k2, v2, k5, v5);
                    }
                }
            } else {
                if (k3 != null && v3 != null) {
                    if (k4 != null && v4 != null) {
                        return of(k3, v3, k4, v4, k5, v5);
                    } else {
                        return of(k3, v3, k5, v5);
                    }
                } else {
                    if (k4 != null && v4 != null) {
                        return of(k4, v4, k5, v5);
                    } else {
                        return empty();
                    }
                }
            }
        }
    }

    public static <K, V> ImmutableMap<K, V> of(Map<K, V> map, K k1, V v1) {
        if (map == null || map.isEmpty()) {
            return of(k1, v1);
        } else if (map.size() == 1) {
            Map.Entry<K, V> entry = map.entrySet().iterator().next();
            return of(entry.getKey(), entry.getValue(), k1, v1);
        } else {
            Map<K, V> copy = new LinkedHashMap<>(map);
            copy.put(k1, v1);
            return new MapBackedMap<>(copy);
        }
    }

    public static <K, V> ImmutableMap<K, V> of(Map<K, V> map, K k1, V v1, K k2, V v2) {
        if (map == null || map.isEmpty()) {
            return of(k1, v1, k2, v2);
        } else {
            Map<K, V> copy = new LinkedHashMap<>(map);
            copy.put(k1, v1);
            copy.put(k2, v2);
            return new MapBackedMap<>(copy);
        }
    }

    public static <K, V> ImmutableMap<K, V> without(Map<K, V> map, K key) {
        if (map.containsKey(key)) {
            if (map.size() == 1) {
                return empty();
            } else {
                return new WithoutMap<K, V>(map, key);
            }
        } else {
            return ImmutableMap.of(map);
        }
    }

    public static <C, K, V> ImmutableMap<K, V> map(Collection<C> collection, Function<C, Map.Entry<K, V>> mappingFunction) {
        ImmutableMap.Builder<K, V> builder = new ImmutableMap.Builder<>(collection.size());

        for (C c : collection) {
            Map.Entry<K, V> entry = mappingFunction.apply(c);

            if (entry != null) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }

        return builder.build();
    }

    public static <KS, VS, KT, VT> ImmutableMap<KT, VT> map(Map<KS, VS> source, Function<KS, KT> keyMappingFunction,
            Function<VS, VT> valueMappingFunction) {
        ImmutableMap.Builder<KT, VT> builder = new ImmutableMap.Builder<>(source.size());

        for (Map.Entry<KS, VS> entry : source.entrySet()) {
            KT newKey = keyMappingFunction.apply(entry.getKey());
            VT newValue = valueMappingFunction.apply(entry.getValue());

            if (newKey != null && newValue != null) {
                builder.put(newKey, newValue);
            }
        }

        return builder.build();
    }

    public static <K, V> Map.Entry<K, V> entry(K k1, V v1) {
        return new AbstractMap.SimpleImmutableEntry<>(k1, v1);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ImmutableMap<K, V> empty() {
        return (ImmutableMap<K, V>) EMPTY_MAP;
    }

    ImmutableMap<K, V> without(K key);

    ImmutableMap<K, V> with(K key, V value);

    ImmutableMap<K, V> with(ImmutableMap<K, V> other);

    ImmutableMap<K, V> withComputed(K key, Function<V, V> f);

    ImmutableMap<K, V> intersection(ImmutableSet<K> keys);

    public static class Builder<K, V> {
        private InternalBuilder<K, V> internalBuilder;

        public Builder() {
            internalBuilder = new HashArray16BackedMap.Builder<K, V>();
        }

        public Builder(int expectedNumberOfElements) {
            if (expectedNumberOfElements < 16) {
                internalBuilder = new HashArray16BackedMap.Builder<K, V>();
            } else {
                internalBuilder = new MapBackedMap.Builder<K, V>(expectedNumberOfElements);
            }
        }

        public Builder(Map<K, V> initialContent) {
            if (initialContent instanceof HashArray16BackedMap) {
                internalBuilder = new HashArray16BackedMap.Builder<K, V>((HashArray16BackedMap<K, V>) initialContent);
            } else {
                if (initialContent.size() < 16) {
                    internalBuilder = new HashArray16BackedMap.Builder<K, V>();

                    for (Map.Entry<K, V> entry : initialContent.entrySet()) {
                        internalBuilder = internalBuilder.with(entry.getKey(), entry.getValue());
                    }
                } else {
                    internalBuilder = new MapBackedMap.Builder<K, V>(initialContent);
                }
            }
        }

        public Builder<K, V> with(K key, V value) {
            if (key == null) {
                throw new IllegalArgumentException("null keys are not allowed");
            }
            internalBuilder = internalBuilder.with(key, value);
            return this;
        }

        public Builder<K, V> with(Map<K, V> map) {
            internalBuilder = internalBuilder.with(map);
            return this;
        }

        public void put(K key, V value) {
            if (key == null) {
                throw new IllegalArgumentException("null keys are not allowed");
            }

            internalBuilder = internalBuilder.with(key, value);
        }

        public void putAll(Map<K, V> map) {
            internalBuilder = internalBuilder.with(map);
        }

        public boolean remove(K e) {
            return internalBuilder.remove(e);
        }

        public boolean contains(K e) {
            return internalBuilder.contains(e);
        }

        public ImmutableMap<K, V> build() {
            return internalBuilder.build();
        }

        public int size() {
            return internalBuilder.size();
        }
    }

    static abstract class InternalBuilder<K, V> {
        abstract InternalBuilder<K, V> with(K key, V value);

        abstract InternalBuilder<K, V> with(Map<K, V> map);

        abstract boolean remove(K e);

        abstract boolean contains(K e);

        abstract ImmutableMap<K, V> build();

        abstract int size();

    }

    static class SingleElementMap<K, V> extends AbstractImmutableMap<K, V> {
        private final K key;
        private final V value;
        private Set<K> keySet;
        private Set<V> values;
        private Set<Entry<K, V>> entrySet;

        SingleElementMap(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsValue(Object value) {
            return Objects.equals(this.value, value);
        }

        @Override
        public boolean containsKey(Object key) {
            return Objects.equals(this.key, key);

        }

        @Override
        public V get(Object key) {
            if (Objects.equals(this.key, key)) {
                return this.value;
            } else {
                return null;
            }
        }

        @Override
        public Set<K> keySet() {
            if (keySet == null) {
                keySet = ImmutableSet.of(this.key);
            }

            return keySet;
        }

        @Override
        public Collection<V> values() {
            if (values == null) {
                values = ImmutableSet.of(this.value);
            }

            return values;
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            if (entrySet == null) {
                entrySet = ImmutableSet.of(new AbstractMap.SimpleEntry<>(key, value));
            }
            return entrySet;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map)) {
                return false;
            }

            Map<?, ?> otherMap = (Map<?, ?>) o;

            if (otherMap.size() != 1) {
                return false;
            }

            Entry<?, ?> entry = otherMap.entrySet().iterator().next();

            return Objects.equals(key, entry.getKey()) && Objects.equals(value, entry.getValue());
        }

        @Override
        public ImmutableMap<K, V> with(K key, V value) {
            if (this.key.equals(key)) {
                if (Objects.equals(this.value, value)) {
                    return this;
                } else {
                    return new SingleElementMap<>(key, value);
                }
            } else {
                return new TwoElementMap<>(this.key, this.value, key, value);
            }
        }

        @Override
        public ImmutableMap<K, V> intersection(ImmutableSet<K> keys) {
            if (keys.contains(this.key)) {
                return this;
            } else {
                return empty();
            }
        }

        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) {
            action.accept(key, value);
        }

    }

    static class TwoElementMap<K, V> extends AbstractImmutableMap<K, V> {
        private final K key1;
        private final V value1;
        private final K key2;
        private final V value2;
        private Set<K> keySet;
        private Set<V> values;
        private Set<Entry<K, V>> entrySet;

        TwoElementMap(K key1, V value1, K key2, V value2) {
            this.key1 = key1;
            this.value1 = value1;
            this.key2 = key2;
            this.value2 = value2;
        }

        @Override
        public int size() {
            return 2;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsValue(Object value) {
            return Objects.equals(this.value1, value) || Objects.equals(this.value2, value);
        }

        @Override
        public boolean containsKey(Object key) {
            return Objects.equals(this.key1, key) || Objects.equals(this.key2, key);

        }

        @Override
        public V get(Object key) {
            if (Objects.equals(this.key1, key)) {
                return this.value1;
            } else if (Objects.equals(this.key2, key)) {
                return this.value2;
            } else {
                return null;
            }
        }

        @Override
        public Set<K> keySet() {
            if (keySet == null) {
                keySet = ImmutableSet.of(this.key1, this.key2);
            }

            return keySet;
        }

        @Override
        public Collection<V> values() {
            if (values == null) {
                values = ImmutableSet.of(this.value1, this.value2);
            }

            return values;
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            if (entrySet == null) {
                entrySet = ImmutableSet.of(new AbstractMap.SimpleEntry<>(key1, value1), new AbstractMap.SimpleEntry<>(key2, value2));
            }
            return entrySet;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map)) {
                return false;
            }

            Map<?, ?> otherMap = (Map<?, ?>) o;

            if (otherMap.size() != 2) {
                return false;
            }

            return Objects.equals(value1, otherMap.get(key1)) && Objects.equals(value2, otherMap.get(key2));
        }

        @Override
        public ImmutableMap<K, V> with(K key, V value) {
            if (Objects.equals(this.key1, key)) {
                if (Objects.equals(this.value1, value)) {
                    return this;
                } else {
                    return new TwoElementMap<>(key1, value, key2, value2);
                }
            } else if (Objects.equals(this.key2, key)) {
                if (Objects.equals(this.value2, value)) {
                    return this;
                } else {
                    return new TwoElementMap<>(key1, value1, key2, value);
                }
            } else {
                return new ArrayBackedMap<>(key1, value1, key2, value2, key, value);
            }
        }

        @Override
        public ImmutableMap<K, V> intersection(ImmutableSet<K> keys) {
            if (keys.contains(key1)) {
                if (keys.contains(key2)) {
                    return this;
                } else {
                    return new SingleElementMap<>(key1, value1);
                }
            } else if (keys.contains(key2)) {
                return new SingleElementMap<>(key2, value2);
            } else {
                return empty();
            }
        }

        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) {
            action.accept(key1, value1);
            action.accept(key2, value2);
        }

    }

    static class HashArray16BackedMap<K, V> extends AbstractImmutableMap<K, V> {

        private static final int TABLE_SIZE = 16;
        private int size = 0;

        private final Object[] table1;
        private final V[] values1;
        private final Object[] table2;
        private final V[] values2;

        private Entry<K, V>[] flatEntries;

        private Set<K> keySet;
        private Collection<V> values;
        private Set<Entry<K, V>> entrySet;

        HashArray16BackedMap(int size, Object[] table1, V[] values1, Object[] table2, V[] values2) {
            this.size = size;
            this.table1 = table1;
            this.values1 = values1;
            this.values2 = values2;
            this.table2 = table2;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsKey(Object o) {
            int pos = hashPosition(o);

            if (o.equals(this.table1[pos])) {
                return true;
            } else if (this.table2 != null && o.equals(this.table2[pos])) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public V get(Object key) {
            int pos = hashPosition(key);

            if (key.equals(this.table1[pos])) {
                return this.values1[pos];
            } else if (this.table2 != null && key.equals(this.table2[pos])) {
                return this.values2[pos];
            } else {
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Set<K> keySet() {
            if (keySet == null) {
                keySet = new ImmutableSet.HashArrayBackedSet<K>(16, size, (K[]) table1, (K[]) table2);
            }

            return keySet;
        }

        @Override
        public Collection<V> values() {
            if (values == null) {
                values = entrySet().stream().map(e -> e.getValue()).collect(Collectors.toList());
            }

            return values;
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            if (entrySet == null) {
                entrySet = new ImmutableSet.ArrayBackedSet<Entry<K, V>>(getFlatEntries());

            }
            return entrySet;
        }

        @Override
        public ImmutableMap<K, V> with(K otherKey, V otherValue) {

            int pos = hashPosition(otherKey);

            if (this.table1[pos] != null) {
                if (otherKey.equals(this.table1[pos])) {
                    // already contained

                    if (Objects.equals(this.values1[pos], otherValue)) {
                        return this;
                    } else {
                        V[] newValues1 = this.values1.clone();
                        newValues1[pos] = otherValue;
                        return new HashArray16BackedMap<K, V>(size, this.table1, newValues1, this.table2, this.values2);
                    }

                } else if (this.table2 != null) {
                    if (this.table2[pos] != null) {
                        if (otherKey.equals(this.table2[pos])) {
                            // already contained
                            if (Objects.equals(this.values2[pos], otherValue)) {
                                return this;
                            } else {
                                V[] newValues2 = this.values2.clone();
                                newValues2[pos] = otherValue;
                                return new HashArray16BackedMap<K, V>(size, this.table1, this.values1, this.table2, newValues2);
                            }
                        } else {
                            return new WithMap<>(this, otherKey, otherValue);
                        }
                    } else {
                        Object[] newTable2 = this.table2.clone();
                        V[] newValues2 = this.values2.clone();
                        newTable2[pos] = otherKey;
                        newValues2[pos] = otherValue;
                        return new HashArray16BackedMap<K, V>(size + 1, this.table1, this.values1, newTable2, newValues2);
                    }
                } else {
                    Object[] newTable2 = new Object[TABLE_SIZE];
                    @SuppressWarnings("unchecked")
                    V[] newValues2 = (V[]) new Object[TABLE_SIZE];

                    newTable2[pos] = otherKey;
                    newValues2[pos] = otherValue;
                    return new HashArray16BackedMap<>(size + 1, this.table1, this.values1, newTable2, newValues2);
                }
            } else {
                Object[] newTable1 = this.table1.clone();
                V[] newValues1 = this.values1.clone();
                newTable1[pos] = otherKey;
                newValues1[pos] = otherValue;
                return new HashArray16BackedMap<K, V>(size + 1, newTable1, newValues1, this.table2, this.values2);
            }
        }

        @Override
        public ImmutableMap<K, V> with(ImmutableMap<K, V> other) {
            int otherSize = other.size();

            if (otherSize == 0) {
                return this;
            } else if (otherSize == 1) {
                Map.Entry<K, V> otherOnly = other.entrySet().iterator().next();
                return this.with(otherOnly.getKey(), otherOnly.getValue());
            } else {
                HashArray16BackedMap.Builder<K, V> builder = new HashArray16BackedMap.Builder<K, V>(this);
                builder.with(other);
                return builder.build();
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public ImmutableMap<K, V> intersection(ImmutableSet<K> keys) {
            int otherSize = keys.size();

            if (otherSize == 0) {
                return empty();
            } else if (otherSize == 1) {
                K key = keys.only();

                if (containsKey(key)) {
                    return new SingleElementMap<>(key, get(key));
                } else {
                    return empty();
                }
            }

            if (keys instanceof ImmutableSet.HashArrayBackedSet && ((ImmutableSet.HashArrayBackedSet<?>) keys).tableSize == 16) {
                return intersection((ImmutableSet.HashArrayBackedSet<K>) keys);
            } else {
                Object[] newTable1 = new Object[TABLE_SIZE];
                V[] newValues1 = (V[]) new Object[TABLE_SIZE];
                int table1count = 0;

                Object[] newTable2 = null;
                V[] newValues2 = null;
                int table2count = 0;

                for (int i = 0; i < TABLE_SIZE; i++) {
                    if (this.table1[i] != null && keys.contains(this.table1[i])) {
                        newTable1[i] = this.table1[i];
                        newValues1[i] = this.values1[i];
                        table1count++;
                    }
                }

                if (table2 != null) {
                    newTable2 = new Object[TABLE_SIZE];
                    newValues2 = (V[]) new Object[TABLE_SIZE];

                    for (int i = 0; i < TABLE_SIZE; i++) {
                        if (this.table2[i] != null && keys.contains(this.table2[i])) {
                            if (newTable1[i] == null) {
                                newTable1[i] = this.table2[i];
                                newValues1[i] = this.values2[i];
                                table1count++;
                            } else {
                                newTable2[i] = this.table2[i];
                                newValues2[i] = this.values2[i];
                                table2count++;
                            }
                        }
                    }
                }

                int total = table1count + table2count;

                if (total == 0) {
                    return empty();
                } else if (total == 1) {
                    int index = findIndexOfNextNonNull(newTable1, 0);
                    return new SingleElementMap<K, V>((K) newTable1[index], newValues1[index]);
                } else {
                    if (table2count == 0) {
                        newTable2 = null;
                        newValues2 = null;
                    }

                    return new HashArray16BackedMap<>(total, newTable1, newValues1, newTable2, newValues2);
                }
            }
        }

        @SuppressWarnings("unchecked")
        private ImmutableMap<K, V> intersection(ImmutableSet.HashArrayBackedSet<K> keys) {

            Object[] newTable1 = new Object[TABLE_SIZE];
            V[] newValues1 = (V[]) new Object[TABLE_SIZE];
            int table1count = 0;

            Object[] newTable2 = null;
            V[] newValues2 = null;
            int table2count = 0;

            for (int i = 0; i < TABLE_SIZE; i++) {
                if (this.table1[i] != null && keys.contains(this.table1[i], i)) {
                    newTable1[i] = this.table1[i];
                    newValues1[i] = this.values1[i];
                    table1count++;
                }
            }

            if (table2 != null) {
                newTable2 = new Object[TABLE_SIZE];
                newValues2 = (V[]) new Object[TABLE_SIZE];

                for (int i = 0; i < TABLE_SIZE; i++) {
                    if (this.table2[i] != null && keys.contains(this.table2[i], i)) {
                        if (newTable1[i] == null) {
                            newTable1[i] = this.table2[i];
                            newValues1[i] = this.values2[i];
                            table1count++;
                        } else {
                            newTable2[i] = this.table2[i];
                            newValues2[i] = this.values2[i];
                            table2count++;
                        }
                    }
                }
            }

            int total = table1count + table2count;

            if (total == 0) {
                return empty();
            } else if (total == 1) {
                int index = findIndexOfNextNonNull(newTable1, 0);
                return new SingleElementMap<K, V>((K) newTable1[index], newValues1[index]);
            } else {
                if (table2count == 0) {
                    newTable2 = null;
                    newValues2 = null;
                }

                return new HashArray16BackedMap<>(total, newTable1, newValues1, newTable2, newValues2);
            }

        }

        @SuppressWarnings("unchecked")
        private Map.Entry<K, V>[] getFlatEntries() {
            if (flatEntries != null) {
                return flatEntries;
            }

            Entry<K, V>[] flatEntries = (Entry<K, V>[]) new Entry[size];

            int k = 0;

            for (int i = 0; i < table1.length; i++) {
                Object key = table1[i];

                if (key != null) {
                    flatEntries[k] = new AbstractMap.SimpleEntry<K, V>((K) key, values1[i]);
                    k++;
                }
            }

            if (table2 != null) {
                for (int i = 0; i < table2.length; i++) {
                    Object key = table2[i];

                    if (key != null) {
                        flatEntries[k] = new AbstractMap.SimpleEntry<K, V>((K) key, values2[i]);
                        k++;
                    }
                }
            }

            this.flatEntries = flatEntries;

            return flatEntries;
        }

        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) {

            for (int i = 0; i < table1.length; i++) {
                Object key = table1[i];

                if (key != null) {
                    action.accept((K) key, values1[i]);
                }
            }

            if (table2 != null) {
                for (int i = 0; i < table2.length; i++) {
                    Object key = table2[i];

                    if (key != null) {
                        action.accept((K) key, values2[i]);
                    }
                }
            }
        }

        static Object findFirstNonNull(Object[] array) {
            for (int i = 0; i < array.length; i++) {
                if (array[i] != null) {
                    return array[i];
                }
            }

            return null;
        }

        static Object findFirstNonNull(Object[] array1, Object[] array2) {
            Object result = findFirstNonNull(array1);

            if (result == null && array2 != null) {
                result = findFirstNonNull(array2);
            }

            return result;
        }

        static int findIndexOfNextNonNull(Object[] array, int start) {
            for (int i = start; i < array.length; i++) {
                if (array[i] != null) {
                    return i;
                }
            }

            return -1;
        }

        static int hashPosition(Object e) {
            if (e == null) {
                throw new IllegalArgumentException("ImmutableMap does not support null keys");
            }

            int hash = e.hashCode();
            int pos = (hash & 0xf) ^ (hash >> 4 & 0xf) ^ (hash >> 8 & 0xf) ^ (hash >> 12 & 0xf) ^ (hash >> 16 & 0xf) ^ (hash >> 20 & 0xf)
                    ^ (hash >> 24 & 0xf) ^ (hash >> 28 & 0xf);
            return pos;
        }

        static class Builder<K, V> extends InternalBuilder<K, V> {
            // TODO sash hashPos of first to save equals calls below
            private K first;
            private V firstValue;
            private Object[] tail1;
            private V[] tail1values;
            private Object[] tail2;
            private V[] tail2values;

            private int size = 0;

            public Builder() {

            }

            @SuppressWarnings("unchecked")
            public Builder(HashArray16BackedMap<K, V> initialContent) {
                this.tail1 = initialContent.table1.clone();
                this.tail1values = initialContent.values1.clone();

                if (initialContent.table2 != null) {
                    this.tail2 = initialContent.table2.clone();
                    this.tail2values = initialContent.values2.clone();
                }

                this.size = initialContent.size;

                int firstIndex = findIndexOfNextNonNull(tail1, size);

                if (firstIndex != -1) {
                    this.first = (K) this.tail1[firstIndex];
                    this.firstValue = this.tail1values[firstIndex];
                    this.tail1[firstIndex] = null;
                }
            }

            public InternalBuilder<K, V> with(K key, V value) {
                if (key == null) {
                    throw new IllegalArgumentException("Null keys are not supported");
                }

                if (first == null) {
                    first = key;
                    firstValue = value;
                    size++;
                    return this;
                } else if (first.equals(key)) {
                    firstValue = value;
                    return this;
                } else if (tail1 == null) {
                    tail1 = new Object[TABLE_SIZE];
                    tail1values = (V[]) new Object[TABLE_SIZE];
                    int pos = hashPosition(key);
                    tail1[pos] = key;
                    tail1values[pos] = value;
                    size++;
                    return this;
                } else {
                    int pos = hashPosition(key);

                    if (tail1[pos] == null) {
                        tail1[pos] = key;
                        tail1values[pos] = value;
                        size++;
                        return this;
                    } else if (tail1[pos].equals(key)) {
                        tail1values[pos] = value;
                        return this;
                    } else {
                        // collision

                        if (tail2 == null) {
                            tail2 = new Object[TABLE_SIZE];
                            tail2values = (V[]) new Object[TABLE_SIZE];
                            tail2[pos] = key;
                            tail2values[pos] = value;
                            size++;
                            return this;
                        } else if (tail2[pos] == null) {
                            tail2[pos] = key;
                            tail2values[pos] = value;
                            size++;
                            return this;
                        } else if (tail2[pos].equals(key)) {
                            tail2values[pos] = value;
                            return this;
                        } else {
                            // collision and overflow

                            HashMap<K, V> jdkMap = buildJdkHashMap();
                            jdkMap.put(key, value);

                            return new MapBackedMap.Builder<>(jdkMap);
                        }
                    }
                }
            }

            private HashMap<K, V> buildJdkHashMap() {
                HashMap<K, V> result = new HashMap<>(size);

                if (first != null) {
                    result.put(first, firstValue);
                }

                if (tail1 != null) {
                    for (int i = 0; i < TABLE_SIZE; i++) {
                        if (tail1[i] != null) {
                            result.put((K) tail1[i], tail1values[i]);
                        }
                    }
                }

                if (tail2 != null) {
                    for (int i = 0; i < TABLE_SIZE; i++) {
                        if (tail2[i] != null) {
                            result.put((K) tail2[i], tail2values[i]);
                        }
                    }
                }

                return result;
            }

            @Override
            InternalBuilder<K, V> with(Map<K, V> map) {
                if (map instanceof HashArray16BackedMap) {
                    return with((HashArray16BackedMap<K, V>) map);
                }

                InternalBuilder<K, V> builder = this;

                for (Map.Entry<K, V> entry : map.entrySet()) {
                    builder = builder.with(entry.getKey(), entry.getValue());
                }

                return builder;
            }

            @SuppressWarnings("unchecked")
            InternalBuilder<K, V> with(HashArray16BackedMap<K, V> map) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    if (map.table1[i] == null) {
                        continue;
                    }

                    K key = (K) map.table1[i];
                    V value = map.values1[i];

                    if (first == null) {
                        first = key;
                        firstValue = value;
                        size++;
                    } else if (first.equals(key)) {
                        firstValue = value;
                    } else if (this.tail1 == null) {
                        tail1 = new Object[TABLE_SIZE];
                        tail1values = (V[]) new Object[TABLE_SIZE];
                        tail1[i] = key;
                        tail1values[i] = value;
                        size++;
                    } else if (this.tail1[i] == null) {
                        this.tail1[i] = key;
                        this.tail1values[i] = value;
                        size++;
                    } else if (this.tail1[i].equals(key)) {
                        this.tail1values[i] = value;
                    } else {
                        // collision

                        if (this.tail2 == null) {
                            tail2 = new Object[TABLE_SIZE];
                            tail2values = (V[]) new Object[TABLE_SIZE];
                            tail2[i] = map.table1[i];
                            tail2values[i] = map.values1[i];
                            size++;
                        } else if (this.tail2[i] == null) {
                            tail2[i] = map.table1[i];
                            tail2values[i] = map.values1[i];
                            size++;
                        } else if (this.tail2[i].equals(map.table1[i])) {
                            tail2values[i] = map.values1[i];
                        } else {
                            // collision and overflow
                            return handleOverflow(new MapBackedMap.Builder<>(build()).with(key, value), map, 1, i);
                        }
                    }
                }

                if (map.table2 != null) {
                    for (int i = 0; i < TABLE_SIZE; i++) {
                        if (map.table2[i] == null) {
                            continue;
                        }

                        K key = (K) map.table2[i];
                        V value = map.values2[i];

                        if (first == null) {
                            first = key;
                            firstValue = value;
                            size++;
                        } else if (first.equals(key)) {
                            firstValue = value;
                        } else if (this.tail1 == null) {
                            tail1 = new Object[TABLE_SIZE];
                            tail1values = (V[]) new Object[TABLE_SIZE];
                            tail1[i] = key;
                            tail1values[i] = value;
                            size++;
                        } else if (this.tail1[i] == null) {
                            this.tail1[i] = key;
                            this.tail1values[i] = value;
                            size++;
                        } else if (this.tail1[i].equals(key)) {
                            this.tail1values[i] = value;
                        } else {
                            // collision

                            if (this.tail2 == null) {
                                tail2 = new Object[TABLE_SIZE];
                                tail2values = (V[]) new Object[TABLE_SIZE];
                                tail2[i] = key;
                                tail2values[i] = value;
                                size++;
                            } else if (this.tail2[i] == null) {
                                tail2[i] = key;
                                tail2values[i] = value;
                                size++;
                            } else if (this.tail2[i].equals(key)) {
                                tail2values[i] = value;
                            } else {
                                // collision and overflow
                                return handleOverflow(new MapBackedMap.Builder<>(build()).with(key, value), map, 2, i);
                            }
                        }
                    }
                }

                return this;
            }

            InternalBuilder<K, V> handleOverflow(InternalBuilder<K, V> newBuilder, HashArray16BackedMap<K, V> map, int state, int startPos) {
                if (state == 1) {
                    for (int i = startPos + 1; i < TABLE_SIZE; i++) {
                        if (map.table1[i] == null) {
                            continue;
                        }

                        @SuppressWarnings("unchecked")
                        K key = (K) map.table1[i];
                        V value = map.values1[i];

                        newBuilder = newBuilder.with(key, value);
                    }
                }

                if (map.table2 != null) {
                    for (int i = state == 2 ? startPos + 1 : 0; i < TABLE_SIZE; i++) {
                        if (map.table2[i] == null) {
                            continue;
                        }

                        @SuppressWarnings("unchecked")
                        K key = (K) map.table2[i];
                        V value = map.values2[i];

                        newBuilder = newBuilder.with(key, value);
                    }
                }

                return newBuilder;
            }

            @SuppressWarnings("unchecked")
            public ImmutableMap<K, V> build() {
                if (size == 0) {
                    return ImmutableMap.empty();
                } else if (first == null) {
                    return new HashArray16BackedMap<>(size, tail1, tail1values, tail2, tail2values);
                } else if (size == 1) {
                    return new SingleElementMap<>(first, firstValue);
                } else if (size == 2) {
                    int i = findIndexOfNextNonNull(tail1, 0);
                    return new TwoElementMap<K, V>(first, firstValue, (K) tail1[i], tail1values[i]);
                } else {
                    // We need to integrate the first element
                    int firstPos = hashPosition(first);

                    if (this.tail1[firstPos] == null) {
                        this.tail1[firstPos] = first;
                        this.tail1values[firstPos] = firstValue;
                        return new HashArray16BackedMap<>(size, tail1, tail1values, tail2, tail2values);
                    } else if (this.tail2 == null) {
                        this.tail2 = new Object[TABLE_SIZE];
                        this.tail2values = (V[]) new Object[TABLE_SIZE];
                        this.tail2[firstPos] = first;
                        this.tail2values[firstPos] = firstValue;
                        return new HashArray16BackedMap<>(size, tail1, tail1values, tail2, tail2values);
                    } else if (this.tail2[firstPos] == null) {
                        this.tail2[firstPos] = first;
                        this.tail2values[firstPos] = firstValue;
                        return new HashArray16BackedMap<>(size, tail1, tail1values, tail2, tail2values);
                    } else {
                        return new WithMap<>(new HashArray16BackedMap<>(size, tail1, tail1values, tail2, tail2values), first, firstValue);
                    }

                }
            }

            @Override
            int size() {
                return size;
            }

            @Override
            boolean remove(K key) {
                if (key.equals(first)) {
                    first = null;
                    size--;

                    return true;
                } else {
                    int position = hashPosition(key);

                    if (tail1[position] != null && tail1[position].equals(key)) {
                        tail1[position] = null;
                        size--;
                        return true;
                    } else if (tail2 != null && tail2[position] != null && tail2[position].equals(key)) {
                        tail2[position] = null;
                        size--;
                        return true;
                    }
                }

                return false;
            }

            @Override
            boolean contains(K key) {
                if (key.equals(first)) {
                    return true;
                } else {
                    int position = hashPosition(key);

                    if (tail1[position] != null && tail1[position].equals(key)) {
                        return true;
                    } else if (tail2 != null && tail2[position] != null && tail2[position].equals(key)) {
                        return true;
                    }
                }

                return false;
            }

        }

    }

    static class ArrayBackedMap<K, V> extends AbstractImmutableMap<K, V> {
        private final Object[] keys;
        private final Object[] values;
        private Set<K> keySet;
        private Set<V> valueSet;
        private Set<Entry<K, V>> entrySet;

        ArrayBackedMap(K k1, V v1, K k2, V v2, K k3, V v3) {
            this.keys = new Object[] { k1, k2, k3 };
            this.values = new Object[] { v1, v2, v3 };
        }

        ArrayBackedMap(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
            this.keys = new Object[] { k1, k2, k3, k4 };
            this.values = new Object[] { v1, v2, v3, v4 };
        }

        ArrayBackedMap(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
            this.keys = new Object[] { k1, k2, k3, k4, k5 };
            this.values = new Object[] { v1, v2, v3, v4, v5 };
        }

        ArrayBackedMap(Map<K, V> map) {
            this.keys = new Object[map.size()];
            this.values = new Object[map.size()];

            int i = 0;

            for (Map.Entry<K, V> entry : map.entrySet()) {
                this.keys[i] = entry.getKey();
                this.values[i] = entry.getValue();
                i++;
            }
        }

        ArrayBackedMap(Object[] keys, Object[] values) {
            this.keys = keys;
            this.values = values;
        }

        @Override
        public int size() {
            return keys.length;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsValue(Object value) {
            for (int i = 0; i < values.length; i++) {
                if (Objects.equals(values[i], value)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public boolean containsKey(Object key) {
            for (int i = 0; i < keys.length; i++) {
                if (Objects.equals(keys[i], key)) {
                    return true;
                }
            }

            return false;
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get(Object key) {
            for (int i = 0; i < keys.length; i++) {
                if (Objects.equals(keys[i], key)) {
                    return (V) values[i];
                }
            }
            return null;

        }

        @SuppressWarnings("unchecked")
        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) {
            for (int i = 0; i < keys.length; i++) {
                action.accept((K) keys[i], (V) values[i]);

            }
        }

        @Override
        public Set<K> keySet() {
            if (keySet == null) {
                keySet = new ImmutableSet.ArrayBackedSet<K>(keys);
            }

            return keySet;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Collection<V> values() {
            if (valueSet == null) {
                valueSet = (Set<V>) ImmutableSet.of(new HashSet<>(Arrays.asList(values)));
            }

            return valueSet;
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            if (entrySet == null) {
                entrySet = new AbstractSet<Entry<K, V>>() {

                    @Override
                    public int size() {
                        return ArrayBackedMap.this.size();
                    }

                    @Override
                    public boolean isEmpty() {
                        return ArrayBackedMap.this.isEmpty();
                    }

                    @Override
                    public boolean contains(Object o) {
                        return ArrayBackedMap.this.containsKey(o);
                    }

                    @Override
                    public Iterator<Entry<K, V>> iterator() {

                        return new Iterator<Entry<K, V>>() {

                            private int i = 0;

                            @Override
                            public boolean hasNext() {
                                return i < keys.length;
                            }

                            @Override
                            public Entry<K, V> next() {
                                if (i < keys.length) {
                                    @SuppressWarnings("unchecked")
                                    Entry<K, V> result = new AbstractMap.SimpleEntry<K, V>((K) keys[i], (V) values[i]);
                                    i++;
                                    return result;
                                } else {
                                    throw new NoSuchElementException();
                                }
                            }

                        };
                    }

                    @Override
                    public boolean add(Entry<K, V> e) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean remove(Object o) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean addAll(Collection<? extends Entry<K, V>> c) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean retainAll(Collection<?> c) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean removeAll(Collection<?> c) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void clear() {
                        throw new UnsupportedOperationException();
                    }

                };
                ;
            }
            return entrySet;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map)) {
                return false;
            }

            Map<?, ?> otherMap = (Map<?, ?>) o;

            if (otherMap.size() != size()) {
                return false;
            }

            for (int i = 0; i < keys.length; i++) {
                if (!Objects.equals(values[i], otherMap.get(keys[i]))) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public ImmutableMap<K, V> with(K key, V value) {

            for (int i = 0; i < keys.length; i++) {
                if (Objects.equals(keys[i], key)) {
                    if (Objects.equals(values[i], value)) {
                        return this;
                    } else {
                        Object[] values = this.values.clone();
                        values[i] = value;
                        return new ArrayBackedMap<>(keys, values);
                    }
                }
            }

            int l = this.keys.length;

            if (l < 4) {
                Object[] keys = new Object[l + 1];
                Object[] values = new Object[l + 1];
                System.arraycopy(this.keys, 0, keys, 0, l);
                System.arraycopy(this.values, 0, values, 0, l);

                keys[l] = key;
                values[l] = value;

                return new ArrayBackedMap<>(keys, values);
            } else {
                Map<K, V> map = new LinkedHashMap<>(this);
                map.put(key, value);
                return new MapBackedMap<>(map);
            }
        }

    }

    static class MapBackedMap<K, V> extends AbstractImmutableMap<K, V> {
        private final Map<K, V> delegate;

        MapBackedMap(Map<K, V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        @Override
        public boolean containsValue(Object value) {
            return delegate.containsValue(value);
        }

        @Override
        public boolean containsKey(Object key) {
            return delegate.containsKey(key);

        }

        @Override
        public V get(Object key) {
            return delegate.get(key);
        }

        @Override
        public Set<K> keySet() {
            return Collections.unmodifiableSet(delegate.keySet());
        }

        @Override
        public Collection<V> values() {
            return Collections.unmodifiableCollection(delegate.values());
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return Collections.unmodifiableSet(delegate.entrySet());
        }

        @Override
        public boolean equals(Object o) {
            return delegate.equals(o);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public ImmutableMap<K, V> with(K key, V value) {
            if (Objects.equals(delegate.get(key), value)) {
                return this;
            } else {
                Map<K, V> map = new LinkedHashMap<>(this);
                map.put(key, value);
                return new MapBackedMap<>(map);
            }
        }

        static class Builder<K, V> extends InternalBuilder<K, V> {
            private HashMap<K, V> delegate;

            Builder(int expectedCapacity) {
                this.delegate = new HashMap<>(expectedCapacity);
            }

            Builder(Map<K, V> map) {
                this.delegate = new HashMap<>(map);
            }

            public Builder<K, V> with(K key, V value) {
                this.delegate.put(key, value);
                return this;
            }

            @Override
            ImmutableMap<K, V> build() {
                return new MapBackedMap<>(this.delegate);
            }

            @Override
            InternalBuilder<K, V> with(Map<K, V> map) {
                this.delegate.putAll(map);
                return this;
            }

            @Override
            int size() {
                return delegate.size();
            }

            @Override
            boolean remove(K e) {
                return delegate.remove(e) != null;
            }

            @Override
            boolean contains(K e) {
                return delegate.containsKey(e);
            }

        }

        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) {
            delegate.forEach(action);
        }

    }

    static class WithoutMap<K, V> extends AbstractImmutableMap<K, V> {
        private final Map<K, V> delegate;
        private final K withoutKey;

        WithoutMap(Map<K, V> delegate, K withoutKey) {
            this.delegate = delegate;
            this.withoutKey = withoutKey;
        }

        @Override
        public int size() {
            return delegate.size() - 1;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsKey(Object key) {
            if (this.withoutKey.equals(key)) {
                return false;
            } else {
                return this.delegate.containsKey(key);
            }
        }

        @Override
        public boolean containsValue(Object value) {
            for (Map.Entry<K, V> entry : delegate.entrySet()) {
                if (this.withoutKey.equals(entry.getKey())) {
                    continue;
                }

                if (Objects.equals(value, entry.getValue())) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public V get(Object key) {
            if (this.withoutKey.equals(key)) {
                return null;
            } else {
                return this.delegate.get(key);
            }
        }

        @Override
        public Set<K> keySet() {
            Set<K> delegateSet = delegate.keySet();

            return new AbstractSet<K>() {

                @Override
                public int size() {
                    return WithoutMap.this.size();
                }

                @Override
                public boolean isEmpty() {
                    return WithoutMap.this.isEmpty();
                }

                @Override
                public boolean contains(Object o) {
                    return WithoutMap.this.containsKey(o);
                }

                @Override
                public Iterator<K> iterator() {
                    Iterator<K> delegateIter = delegateSet.iterator();

                    return new Iterator<K>() {

                        private K next;
                        private boolean initialized;

                        @Override
                        public boolean hasNext() {
                            init();
                            return next != null;
                        }

                        @Override
                        public K next() {
                            init();
                            initialized = false;
                            return next;
                        }

                        private void init() {
                            if (!initialized) {
                                next = null;
                                while (delegateIter.hasNext()) {
                                    next = delegateIter.next();

                                    if (!withoutKey.equals(next)) {
                                        break;
                                    } else {
                                        next = null;
                                    }
                                }

                                initialized = true;
                            }
                        }

                    };
                }

                @Override
                public boolean add(K e) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean remove(Object o) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean addAll(Collection<? extends K> c) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean retainAll(Collection<?> c) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean removeAll(Collection<?> c) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void clear() {
                    throw new UnsupportedOperationException();
                }

            };
        }

        @Override
        public Collection<V> values() {
            return entrySet().stream().map(e -> e.getValue()).collect(Collectors.toSet());
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            Set<Entry<K, V>> delegateSet = delegate.entrySet();

            return new AbstractSet<Entry<K, V>>() {

                @Override
                public int size() {
                    return WithoutMap.this.size();
                }

                @Override
                public boolean isEmpty() {
                    return WithoutMap.this.isEmpty();
                }

                @Override
                public boolean contains(Object o) {
                    return WithoutMap.this.containsKey(o);
                }

                @Override
                public Iterator<Entry<K, V>> iterator() {
                    Iterator<Entry<K, V>> delegateIter = delegateSet.iterator();

                    return new Iterator<Entry<K, V>>() {

                        private Entry<K, V> next;
                        private boolean initialized;

                        @Override
                        public boolean hasNext() {
                            init();
                            return next != null;
                        }

                        @Override
                        public Entry<K, V> next() {
                            init();
                            initialized = false;
                            return next;
                        }

                        private void init() {
                            if (!initialized) {
                                next = null;
                                while (delegateIter.hasNext()) {
                                    next = delegateIter.next();

                                    if (!withoutKey.equals(next.getKey())) {
                                        break;
                                    } else {
                                        next = null;
                                    }
                                }

                                initialized = true;
                            }
                        }

                    };
                }

                @Override
                public boolean add(Entry<K, V> e) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean remove(Object o) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean addAll(Collection<? extends Entry<K, V>> c) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean retainAll(Collection<?> c) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean removeAll(Collection<?> c) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void clear() {
                    throw new UnsupportedOperationException();
                }

            };
        }

        @Override
        public ImmutableMap<K, V> with(K key, V value) {
            if (Objects.equals(delegate.get(key), value)) {
                return this;
            } else {
                Map<K, V> map = new LinkedHashMap<>(this);
                map.put(key, value);
                return new MapBackedMap<>(map);
            }
        }
    }

    static class WithMap<K, V> extends AbstractImmutableMap<K, V> {

        private final ImmutableMap<K, V> base;
        private final K additional;
        private final Map.Entry<K, V> additionalEntry;
        private final int size;

        WithMap(ImmutableMap<K, V> base, K additional, V additionalValue) {
            this.base = base;
            this.additional = additional;
            this.additionalEntry = new AbstractMap.SimpleEntry<K, V>(additional, additionalValue);
            this.size = base.size() + 1;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public V get(Object key) {
            if (additional.equals(key)) {
                return additionalEntry.getValue();
            } else {
                return base.get(key);
            }
        }

        @Override
        public boolean containsKey(Object o) {
            return Objects.equals(o, additional) || this.base.containsKey(o);
        }

        @Override
        public int hashCode() {
            return base.hashCode() + additional.hashCode();
        }

        @Override
        public ImmutableMap<K, V> with(K key, V value) {
            int size = size();

            if (size == 0) {
                return new SingleElementMap<K, V>(key, value);
            } else if (Objects.equals(get(key), value)) {
                return this;
            } else {
                return new Builder<>(this).with(key, value).build();
            }
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return ((ImmutableSet<Entry<K, V>>) base.entrySet()).with(additionalEntry);
        }

        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) {
            base.forEach(action);
            action.accept(additional, additionalEntry.getValue());
        }
    }

    static final Map<?, ?> EMPTY_MAP = new AbstractImmutableMap<Object, Object>() {

        @Override
        public Set<Entry<Object, Object>> entrySet() {
            return Collections.emptySet();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public boolean containsValue(Object value) {
            return false;
        }

        @Override
        public boolean containsKey(Object key) {
            return false;
        }

        @Override
        public Object get(Object key) {
            return null;
        }

        @Override
        public Set<Object> keySet() {
            return Collections.emptySet();
        }

        @Override
        public Collection<Object> values() {
            return Collections.emptySet();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map)) {
                return false;
            }

            Map<?, ?> otherMap = (Map<?, ?>) o;

            return otherMap.size() == 0;
        }

        @Override
        public String toString() {
            return "{}";
        }

        @Override
        public ImmutableMap<Object, Object> with(Object key, Object value) {
            return ImmutableMap.of(key, value);
        }
        
        @Override
        public ImmutableMap<Object, Object> with(ImmutableMap<Object, Object> other) {
            return other;
        }
    };

    abstract static class AbstractImmutableMap<K, V> extends AbstractMap<K, V> implements ImmutableMap<K, V> {

        @Deprecated
        @Override
        public V put(K key, V value) {
            throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public V remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
            throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        public ImmutableMap<K, V> without(K key) {
            if (containsKey(key)) {
                if (size() == 1) {
                    return empty();
                } else {
                    return new WithoutMap<K, V>(this, key);
                }
            } else {
                return this;
            }
        }

        @Override
        public ImmutableMap<K, V> withComputed(K key, Function<V, V> f) {
            V oldValue = this.get(key);
            V newValue = f.apply(oldValue);

            if (Objects.equals(oldValue, newValue)) {
                return this;
            } else {
                return with(key, newValue);
            }
        }

        @Override
        public ImmutableMap<K, V> with(ImmutableMap<K, V> other) {
            if (this.size() == 0) {
                return other;
            } else if (other.size() == 0) {
                return this;
            } else {
                Map<K, V> map = new LinkedHashMap<>(this);
                map.putAll(other);
                return new MapBackedMap<>(map);
            }
        }

        @Override
        public ImmutableMap<K, V> intersection(ImmutableSet<K> keys) {
            int otherSize = keys.size();

            if (otherSize == 0) {
                return empty();
            } else if (otherSize == 1) {
                K otherKey = keys.only();

                if (containsKey(otherKey)) {
                    return new SingleElementMap<>(otherKey, get(otherKey));
                } else {
                    return empty();
                }
            } else {
                Builder<K, V> builder = new Builder<>();

                for (K key : keys) {
                    if (containsKey(key)) {
                        builder.with(key, get(key));
                    }
                }

                return builder.build();
            }

        }

    }

}
