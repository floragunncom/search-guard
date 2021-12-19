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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.Objects;

public interface ImmutableSet<E> extends Set<E> {

    public static <E> ImmutableSet<E> empty() {
        @SuppressWarnings("unchecked")
        ImmutableSet<E> result = (ImmutableSet<E>) EmptySet.INSTANCE;
        return result;
    }

    public static <E> ImmutableSet<E> of(E e) {
        return new OneElementSet<E>(e);
    }

    public static <E> ImmutableSet<E> of(E e1, E e2) {
        if (Objects.equal(e1, e2)) {
            return new OneElementSet<E>(e1);
        } else {
            return new TwoElementSet<E>(e1, e2);
        }
    }

    @SafeVarargs
    public static <E> ImmutableSet<E> of(E e, E... more) {
        if (more == null || more.length == 0) {
            return new OneElementSet<E>(e);
        } else {
            Set<E> moreSet = new HashSet<>(Arrays.asList(more));
            return of(moreSet, e);
        }
    }

    public static <E> ImmutableSet<E> of(Set<E> set, E other) {
        if (set == null || set.size() == 0) {
            return new OneElementSet<E>(other);
        } else if (set.size() == 1) {
            if (other.equals(set.iterator().next())) {
                if (set instanceof ImmutableSet) {
                    return (ImmutableSet<E>) set;
                } else {
                    return new OneElementSet<E>(other);
                }
            } else {
                return new TwoElementSet<E>(set.iterator().next(), other);
            }
        } else if (set.contains(other)) {
            if (set instanceof ImmutableSet) {
                return (ImmutableSet<E>) set;
            } else {
                return new SetBackedSet<>(set);
            }
        } else {
            Set<E> modifiedSet = new HashSet<>(set);
            modifiedSet.add(other);

            if (modifiedSet.size() <= 4) {
                return new ArrayBackedSet<>(modifiedSet);
            } else {
                return new SetBackedSet<>(modifiedSet);
            }
        }
    }

    public static <E> ImmutableSet<E> of(Set<E> set) {
        if (set instanceof ImmutableSet) {
            return (ImmutableSet<E>) set;
        } else if (set == null || set.size() == 0) {
            return empty();
        } else if (set.size() == 1) {
            return new OneElementSet<E>(set.iterator().next());
        } else if (set.size() == 2) {
            Iterator<E> iter = set.iterator();
            return new TwoElementSet<E>(iter.next(), iter.next());
        } else if (set.size() <= 4) {
            return new ArrayBackedSet<>(set);
        } else {
            return new SetBackedSet<>(new HashSet<>(set));
        }
    }

    ImmutableSet<E> with(E other);

    ImmutableSet<E> with(ImmutableSet<E> other);

    static abstract class AbstractImmutableSet<E> implements ImmutableSet<E> {

        public ImmutableSet<E> with(E other) {
            int size = size();

            if (size == 0) {
                return new OneElementSet<E>(other);
            } else if (size == 1) {
                E onlyElement = only();

                if (other.equals(onlyElement)) {
                    return this;
                } else {
                    return new TwoElementSet<E>(onlyElement, other);
                }
            } else if (contains(other)) {
                return this;
            } else {
                Set<E> modifiedSet = new HashSet<>(this);
                modifiedSet.add(other);

                if (modifiedSet.size() <= 4) {
                    return new ArrayBackedSet<>(modifiedSet);
                } else {
                    return new SetBackedSet<>(modifiedSet);
                }
            }
        }

        public ImmutableSet<E> with(ImmutableSet<E> other) {
            int size = size();
            int otherSize = other.size();

            if (size == 0) {
                return other;
            } else if (otherSize == 0) {
                return this;
            } else if (size == 1) {
                return other.with(only());
            } else if (other.size() == 1 && other instanceof AbstractImmutableSet) {
                return with(((AbstractImmutableSet<E>) other).only());
            } else if (size >= otherSize) {
                if (containsAll(other)) {
                    return this;
                }
            } else if (other.containsAll(this)) {
                return other;
            }

            Set<E> modifiedSet = new HashSet<>(this);
            modifiedSet.addAll(other);

            if (modifiedSet.size() <= 4) {
                return new ArrayBackedSet<>(modifiedSet);
            } else {
                return new SetBackedSet<>(modifiedSet);
            }
        }

        E any() {
            return iterator().next();
        }

        E only() {
            if (size() != 1) {
                throw new IllegalStateException();
            }

            return iterator().next();
        }

        @Override
        public boolean add(E e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
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
    }

    static class OneElementSet<E> extends AbstractImmutableSet<E> {

        private final E e1;

        OneElementSet(E e1) {
            this.e1 = e1;
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
        public boolean contains(Object o) {
            return e1.equals(o);
        }

        @Override
        E any() {
            return e1;
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<E>() {

                private int i = 0;

                @Override
                public boolean hasNext() {
                    return i < 1;
                }

                @Override
                public E next() {
                    if (i == 0) {
                        i++;
                        return e1;
                    } else {
                        throw new NoSuchElementException();
                    }
                }
            };
        }

        @Override
        public Object[] toArray() {
            return new Object[] { e1 };
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            if (c.size() == 0) {
                return true;
            } else if (c instanceof Set && c.size() > 1) {
                return false;
            }

            for (Object other : c) {
                if (e1 != other) {
                    return false;
                }
            }

            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T[] toArray(T[] a) {
            T[] result = a.length >= 1 ? a : (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), 1);

            result[0] = (T) e1;

            return result;
        }

    }

    static class TwoElementSet<E> extends AbstractImmutableSet<E> {

        private final E e1;
        private final E e2;

        TwoElementSet(E e1, E e2) {
            this.e1 = e1;
            this.e2 = e2;

            if (e1.equals(e2)) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        E any() {
            return e1;
        }

        @Override
        E only() {
            throw new IllegalStateException();
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
        public boolean contains(Object o) {
            return e1.equals(o) || e2.equals(o);
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<E>() {

                private int i = 0;

                @Override
                public boolean hasNext() {
                    return i < 2;
                }

                @Override
                public E next() {
                    if (i == 0) {
                        i++;
                        return e1;
                    } else if (i == 1) {
                        i++;
                        return e2;
                    } else {
                        throw new NoSuchElementException();
                    }
                }
            };
        }

        @Override
        public Object[] toArray() {
            return new Object[] { e1, e2 };
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T[] toArray(T[] a) {
            T[] result = a.length >= 2 ? a : (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), 2);

            result[0] = (T) e1;
            result[1] = (T) e2;

            return result;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            if (c.size() == 0) {
                return true;
            } else if (c instanceof Set && c.size() > 2) {
                return false;
            }

            for (Object other : c) {
                if (e1 != other && e2 != other) {
                    return false;
                }
            }

            return true;
        }

    }

    static class ArrayBackedSet<E> extends AbstractImmutableSet<E> {

        private final Object[] elements;

        ArrayBackedSet(Object[] elements) {
            this.elements = elements;
        }

        ArrayBackedSet(Set<E> elements) {
            this.elements = elements.toArray();
        }

        @Override
        public int size() {
            return elements.length;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            for (int i = 0; i < elements.length; i++) {
                if (elements[i] == o) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<E>() {

                private int i = 0;

                @Override
                public boolean hasNext() {
                    return i < elements.length;
                }

                @Override
                public E next() {
                    if (i >= elements.length) {
                        throw new NoSuchElementException();

                    }

                    @SuppressWarnings("unchecked")
                    E element = (E) elements[i];
                    i++;
                    return element;
                }
            };
        }

        @Override
        public Object[] toArray() {
            Object[] result = new Object[elements.length];
            System.arraycopy(elements, 0, result, 0, elements.length);
            return result;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T[] toArray(T[] a) {
            T[] result = a.length >= elements.length ? a
                    : (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), elements.length);

            System.arraycopy(elements, 0, result, 0, elements.length);

            return result;
        }

        @SuppressWarnings("unchecked")
        @Override
        E any() {
            return (E) elements[0];
        }

        @SuppressWarnings("unchecked")
        @Override
        E only() {
            if (size() != 1) {
                throw new IllegalStateException();
            }

            return (E) elements[0];
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            if (c.size() == 0) {
                return true;
            } else if (c instanceof Set && c.size() > elements.length) {
                return false;
            }

            for (Object other : c) {
                if (!contains(other)) {
                    return false;
                }
            }

            return true;
        }
    }

    static class SetBackedSet<E> extends AbstractImmutableSet<E> {

        private final Set<E> elements;

        SetBackedSet(Set<E> elements) {
            this.elements = elements;
        }

        @Override
        public int size() {
            return elements.size();
        }

        @Override
        public boolean isEmpty() {
            return elements.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return elements.contains(o);
        }

        @Override
        public Iterator<E> iterator() {
            Iterator<E> delegate = elements.iterator();

            return new Iterator<E>() {

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public E next() {
                    return delegate.next();
                }
            };
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return elements.toArray(a);

        }

        @Override
        public Object[] toArray() {
            return elements.toArray();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return elements.containsAll(c);
        }

    }

    static class EmptySet<E> extends AbstractImmutableSet<E> {

        static EmptySet<?> INSTANCE = new EmptySet<Object>();

        EmptySet() {
        }

        @Override
        E any() {
            throw new IllegalStateException();
        }

        @Override
        E only() {
            throw new IllegalStateException();
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
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public Iterator<E> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        public Object[] toArray() {
            return new Object[] {};
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return a;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            if (c.isEmpty()) {
                return true;
            } else {
                return false;
            }
        }

    }
}
