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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public interface ImmutableMap<K, V> extends Map<K, V> {

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
    };

    abstract static class AbstractImmutableMap<K, V> extends AbstractMap<K, V> implements ImmutableMap<K, V> {

        @Override
        public V put(K key, V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public V remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }
    }
}
