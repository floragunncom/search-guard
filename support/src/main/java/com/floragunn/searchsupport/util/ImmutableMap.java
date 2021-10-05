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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
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
        if (k1 != null && v1 != null) {
            if (k2 != null && v2 != null) {
                if (k3 != null && v3 != null) {
                    return of(k1, v1, k2, v2, k3, v3);
                } else {
                    return of(k1, v1, k2, v2);
                }
            } else {
                if (k3 != null && v3 != null) {
                    return of(k1, v1, k3, v3);
                } else {
                    return of(k1, v1);
                }
            }
        } else {
            if (k2 != null && v2 != null) {
                if (k3 != null && v3 != null) {
                    return of(k2, v2, k3, v3);
                } else {
                    return of(k2, v2);
                }
            } else {
                if (k3 != null && v3 != null) {
                    return of(k3, v3);
                } else {
                    return empty();
                }
            }
        }
    }

    public static <K, V> ImmutableMap<K, V> ofNonNull(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        if (k1 != null && v1 != null) {
            if (k2 != null && v2 != null) {
                if (k3 != null && v3 != null) {
                    if (k4 != null && v4 != null) {
                        return of(k1, v1, k2, v2, k3, v3, k4, v4);
                    } else {
                        return of(k1, v1, k2, v2, k3, v3);
                    }
                } else {
                    if (k4 != null && v4 != null) {
                        return of(k1, v1, k2, v2, k4, v4);
                    } else {
                        return of(k1, v1, k2, v2);
                    }
                }
            } else {
                if (k3 != null && v3 != null) {
                    if (k4 != null && v4 != null) {
                        return of(k1, v1, k3, v3, k4, v4);
                    } else {
                        return of(k1, v1, k3, v3);
                    }
                } else {
                    if (k4 != null && v4 != null) {
                        return of(k1, v1, k4, v4);
                    } else {
                        return of(k1, v1);
                    }
                }
            }
        } else {
            if (k2 != null && v2 != null) {
                if (k3 != null && v3 != null) {
                    if (k4 != null && v4 != null) {
                        return of(k2, v2, k3, v3, k4, v4);
                    } else {
                        return of(k2, v2, k3, v3);
                    }
                } else {
                    if (k4 != null && v4 != null) {
                        return of(k2, v2, k4, v4);
                    } else {
                        return of(k2, v2);
                    }
                }
            } else {
                if (k3 != null && v3 != null) {
                    if (k4 != null && v4 != null) {
                        return of(k3, v3, k4, v4);
                    } else {
                        return of(k3, v3);
                    }
                } else {
                    if (k4 != null && v4 != null) {
                        return of(k4, v4);
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

    public static <K, V> Map<K, V> without(Map<K, V> map, K key) {
        if (map.containsKey(key)) {
            if (map.size() == 1) {
                return Collections.emptyMap();
            } else {
                return new WithoutMap<K, V>(map, key);
            }
        } else {
            return map;
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ImmutableMap<K, V> empty() {
        return (ImmutableMap<K, V>) EMPTY_MAP;
    }

    ImmutableMap<K, V> without(K key);

    ImmutableMap<K, V> with(K key, V value);

    ImmutableMap<K, V> with(ImmutableMap<K, V> other);

    ImmutableMap<K, V> withComputed(K key, Function<V, V> f);

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
                values[l] = values;

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

    }
}
