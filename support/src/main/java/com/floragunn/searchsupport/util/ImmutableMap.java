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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public interface ImmutableMap<K, V> extends Map<K, V> {

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
