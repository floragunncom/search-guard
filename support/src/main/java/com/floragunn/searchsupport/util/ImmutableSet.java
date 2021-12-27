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
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

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
        if (Objects.equals(e1, e2)) {
            return new OneElementSet<E>(e1);
        } else {
            return new TwoElementSet<E>(e1, e2);
        }
    }

    @SafeVarargs
    public static <E> ImmutableSet<E> of(E e, E... more) {
        if (e == null) {
            return ofArray(more);
        } else if (more == null || more.length == 0) {
            return new OneElementSet<E>(e);
        } else {
            return new Builder<>(Arrays.asList(more)).with(e).build();
        }
    }

    @SafeVarargs
    public static <E> ImmutableSet<E> ofArray(E... more) {
        if (more == null || more.length == 0) {
            return empty();
        } else if (more.length == 1 || (more.length == 2 && Objects.equals(more[0], more[1]))) {
            return new OneElementSet<>(more[0]);
        } else if (more.length == 2) {
            return new TwoElementSet<>(more[0], more[1]);
        } else {
            return new Builder<>(Arrays.asList(more)).build();
        }
    }

    public static <E> ImmutableSet<E> of(Collection<E> collection) {
        if (collection == null || collection.size() == 0) {
            return empty();
        } else if (collection instanceof ImmutableSet) {
            return (ImmutableSet<E>) collection;
        } else if (collection.size() == 1) {
            return new OneElementSet<>(collection.iterator().next());
        } else if (collection.size() == 2) {
            Iterator<E> iter = collection.iterator();
            E e1 = iter.next();
            E e2 = iter.next();

            if (Objects.equals(e1, e2)) {
                return new OneElementSet<>(e1);
            } else {
                return new TwoElementSet<>(e1, e2);
            }
        } else {
            return new Builder<>(collection).build();
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
                return new Builder<>(set).build();
            }
        } else {
            return new Builder<>(set).with(other).build();
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
            return new Builder<>(set).build();
        }
    }

    ImmutableSet<E> with(E other);

    ImmutableSet<E> with(Collection<E> other);

    ImmutableSet<E> with(@SuppressWarnings("unchecked") E... other);

    ImmutableSet<E> matching(Predicate<E> predicate);

    default ImmutableSet<E> withoutMatching(Predicate<E> predicate) {
        return matching(predicate.negate());
    }

    ImmutableSet<E> intersection(Set<E> other);

    ImmutableSet<E> without(Collection<E> other);

    ImmutableSet<E> without(E other);

    boolean forAll(Predicate<E> predicate);

    E only();

    String toShortString();

    public static class Builder<E> {
        private InternalBuilder<E> internalBuilder;

        public Builder() {
            internalBuilder = new HashArray16BackedSet.Builder<E>();
        }

        public Builder(int expectedNumberOfElements) {
            if (expectedNumberOfElements < 16) {
                internalBuilder = new HashArray16BackedSet.Builder<E>();
            } else {
                internalBuilder = new SetBackedSet.Builder<E>(expectedNumberOfElements);
            }
        }

        public Builder(Collection<E> initialContent) {
            if (initialContent instanceof HashArray16BackedSet) {
                internalBuilder = new HashArray16BackedSet.Builder<E>((HashArray16BackedSet<E>) initialContent);
            } else {
                if (initialContent.size() < 16) {
                    internalBuilder = new HashArray16BackedSet.Builder<E>();

                    for (E e : initialContent) {
                        internalBuilder = internalBuilder.with(e);
                    }
                } else {
                    internalBuilder = new SetBackedSet.Builder<E>(initialContent);
                }
            }
        }

        public Builder(E[] initialContent) {
            if (initialContent.length < 16) {
                internalBuilder = new HashArray16BackedSet.Builder<E>();

                for (int i = 0; i < initialContent.length; i++) {
                    internalBuilder = internalBuilder.with(initialContent[i]);
                }
            } else {
                internalBuilder = new SetBackedSet.Builder<E>(Arrays.asList(initialContent));
            }
        }

        public Builder<E> with(E e) {
            internalBuilder = internalBuilder.with(e);
            return this;
        }

        public Builder<E> with(Collection<E> collection) {
            internalBuilder = internalBuilder.with(collection);
            return this;
        }

        public boolean add(E e) {
            int size = internalBuilder.size();
            internalBuilder = internalBuilder.with(e);
            return size != internalBuilder.size();
        }

        public boolean addAll(Collection<E> collection) {
            int size = internalBuilder.size();

            for (E e : collection) {
                internalBuilder = internalBuilder.with(e);
            }

            return size != internalBuilder.size();
        }

        public boolean remove(E e) {
            return internalBuilder.remove(e);
        }

        public void clear() {
            internalBuilder.clear();
        }

        public boolean contains(E e) {
            return internalBuilder.contains(e);
        }

        public boolean containsAny(Set<E> set) {
            return internalBuilder.containsAny(set);
        }

        public boolean containsAll(Set<E> set) {
            return internalBuilder.containsAll(set);
        }

        public ImmutableSet<E> build() {
            return internalBuilder.build();
        }

        public Iterator<E> iterator() {
            return internalBuilder.iterator();
        }

        public int size() {
            return internalBuilder.size();
        }

        public E any() {
            return internalBuilder.any();
        }
    }

    static abstract class InternalBuilder<E> {
        abstract InternalBuilder<E> with(E e);

        abstract InternalBuilder<E> with(Collection<E> e);

        abstract boolean remove(E e);

        abstract void clear();

        abstract boolean contains(E e);

        abstract boolean containsAny(Set<E> set);

        abstract boolean containsAll(Set<E> set);

        abstract ImmutableSet<E> build();

        abstract int size();

        abstract Iterator<E> iterator();

        abstract E any();
    }

    static abstract class AbstractImmutableSet<E> extends AbstractImmutableCollection<E> implements ImmutableSet<E> {

        private int hashCode = -1;

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
                return new Builder<>(this).with(other).build();
            }
        }

        public ImmutableSet<E> with(Collection<E> other) {
            if (other instanceof ImmutableSet) {
                return with((ImmutableSet<E>) other);
            }

            int size = size();
            int otherSize = other.size();

            if (size == 0) {
                return ImmutableSet.of(other);
            } else if (otherSize == 0) {
                return this;
            } else if (other.size() == 1) {
                return with(other.iterator().next());
            } else if (size >= otherSize) {
                if (containsAll(other)) {
                    return this;
                }
            }

            return new Builder<E>(this).with(other).build();
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
            } else if (other.size() == 1) {
                return with(other.only());
            } else if (size >= otherSize) {
                if (containsAll(other)) {
                    return this;
                }
            } else if (other.containsAll(this)) {
                return other;
            }

            return new Builder<E>(this).with(other).build();
        }

        @SuppressWarnings("unchecked")
        public ImmutableSet<E> with(E... other) {
            if (other == null || other.length == 0) {
                return this;
            }

            int size = size();
            int otherSize = other.length;

            if (size == 0) {
                return ImmutableSet.ofArray(other);
            } else if (otherSize == 1) {
                return with(other[0]);
            }

            return new Builder<E>(this).with(Arrays.asList(other)).build();
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }

            if (o instanceof Set) {
                Set<?> otherSet = (Set<?>) o;

                return otherSet.size() == this.size() && containsAll(otherSet);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            if (hashCode == -1) {
                int newHashCode = 0;

                for (E e : this) {
                    newHashCode += e.hashCode();
                }

                this.hashCode = newHashCode;
            }

            return this.hashCode;
        }

        @Override
        public ImmutableSet<E> without(E other) {
            if (contains(other)) {
                return matching((e) -> !e.equals(other));
            } else {
                return this;
            }
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            if (c.size() == 0) {
                return true;
            } else if (c instanceof Set && c.size() > size()) {
                return false;
            }

            for (Object other : c) {
                if (!contains(other)) {
                    return false;
                }
            }

            return true;
        }

        public boolean forAll(Predicate<E> predicate) {
            for (E e : this) {
                if (!predicate.test(e)) {
                    return false;
                }
            }

            return true;
        }

    }

    static class OneElementSet<E> extends AbstractImmutableSet<E> {

        private final E e1;

        OneElementSet(E e1) {
            if (e1 == null) {
                throw new IllegalArgumentException("Null elements are not supported");
            }

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
        public E only() {
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

        @Override
        public int hashCode() {
            return e1.hashCode();
        }

        @Override
        public ImmutableSet<E> matching(Predicate<E> predicate) {
            if (predicate.test(e1)) {
                return this;
            } else {
                return empty();
            }
        }

        @Override
        public ImmutableSet<E> intersection(Set<E> other) {
            if (other.contains(this.e1)) {
                return this;
            } else {
                return empty();
            }
        }

        @Override
        public ImmutableSet<E> without(Collection<E> other) {
            if (other.contains(this.e1)) {
                return empty();
            } else {
                return this;
            }
        }

        @Override
        public String toString() {
            if (cachedToString == null) {
                cachedToString = "[" + e1 + "]";
            }

            return cachedToString;
        }

        @Override
        public String toShortString() {
            return toString();
        }

        @Override
        public ImmutableSet<E> without(E other) {
            if (e1.equals(other)) {
                return empty();
            } else {
                return this;
            }
        }

        @Override
        public boolean forAll(Predicate<E> predicate) {
            return predicate.test(e1);
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
        public E only() {
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

        @Override
        public int hashCode() {
            return e1.hashCode() + e2.hashCode();
        }

        @Override
        public ImmutableSet<E> matching(Predicate<E> predicate) {
            if (predicate.test(e1)) {
                if (predicate.test(e2)) {
                    return this;
                } else {
                    return new OneElementSet<>(e1);
                }
            } else if (predicate.test(e2)) {
                return new OneElementSet<>(e2);
            } else {
                return empty();
            }
        }

        @Override
        public ImmutableSet<E> intersection(Set<E> other) {
            if (other.contains(this.e1)) {
                if (other.contains(this.e2)) {
                    return this;
                } else {
                    return new OneElementSet<>(e1);
                }
            } else if (other.contains(this.e2)) {
                return new OneElementSet<>(e2);
            } else {
                return empty();
            }
        }

        @Override
        public ImmutableSet<E> without(Collection<E> other) {
            if (other.contains(this.e1)) {
                if (other.contains(this.e2)) {
                    return empty();
                } else {
                    return new OneElementSet<>(e2);
                }
            } else if (other.contains(this.e2)) {
                return new OneElementSet<>(e1);
            } else {
                return this;
            }
        }

        @Override
        public String toString() {
            if (cachedToString == null) {
                cachedToString = "[" + e1 + "," + e2 + "]";
            }

            return cachedToString;
        }

        @Override
        public String toShortString() {
            return toString();
        }

        @Override
        public ImmutableSet<E> without(E other) {
            if (e1.equals(other)) {
                return new OneElementSet<>(e2);
            } else if (e2.equals(other)) {
                return new OneElementSet<>(e1);
            } else {
                return this;
            }
        }

        @Override
        public boolean forAll(Predicate<E> predicate) {
            return predicate.test(e1) && predicate.test(e2);
        }
    }

    static class ArrayBackedSet<E> extends AbstractImmutableSet<E> {

        private final Object[] elements;
        private String cachedToShortString;

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
                if (elements[i].equals(o)) {
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
        public E only() {
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

        @SuppressWarnings("unchecked")
        @Override
        public ImmutableSet<E> matching(Predicate<E> predicate) {
            Object[] newElements = new Object[this.elements.length];

            int k = 0;

            for (int i = 0; i < this.elements.length; i++) {
                E e = (E) this.elements[i];
                if (predicate.test(e)) {
                    newElements[k] = e;
                    k++;
                }
            }

            if (k == 0) {
                return empty();
            } else if (k == 1) {
                return new OneElementSet<E>((E) newElements[0]);
            } else if (k == 2) {
                return new TwoElementSet<E>((E) newElements[0], (E) newElements[1]);
            } else if (k < this.elements.length) {
                Object[] newElements2 = new Object[k];
                System.arraycopy(newElements, 0, newElements2, 0, k);
                return new ArrayBackedSet<E>(elements);
            } else {
                return this;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public ImmutableSet<E> intersection(Set<E> other) {
            if (other.isEmpty()) {
                return empty();
            }

            if (other instanceof ImmutableSet && other.size() < this.size()) {
                return ((ImmutableSet<E>) other).intersection(this);
            }
            Object[] newElements = new Object[this.elements.length];

            int k = 0;

            for (int i = 0; i < this.elements.length; i++) {
                E e = (E) this.elements[i];
                if (other.contains(e)) {
                    newElements[k] = e;
                    k++;
                }
            }

            if (k == 0) {
                return empty();
            } else if (k == 1) {
                return new OneElementSet<E>((E) newElements[0]);
            } else if (k == 2) {
                return new TwoElementSet<E>((E) newElements[0], (E) newElements[1]);
            } else if (k < this.elements.length) {
                Object[] newElements2 = new Object[k];
                System.arraycopy(newElements, 0, newElements2, 0, k);
                return new ArrayBackedSet<E>(elements);
            } else {
                return this;
            }
        }

        @Override
        public ImmutableSet<E> without(Collection<E> other) {
            if (other.isEmpty()) {
                return this;
            }

            return matching((e) -> !other.contains(e));
        }

        @Override
        public String toShortString() {
            if (cachedToShortString == null) {
                StringBuilder result = new StringBuilder("[");

                for (int i = 0; i < this.elements.length; i++) {
                    if (i != 0) {
                        result.append(", ");
                    }
                    result.append(this.elements[i]);
                }

                result.append("]");

                cachedToShortString = result.toString();
            }

            return cachedToShortString;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean forAll(Predicate<E> predicate) {
            for (int i = 0; i < this.elements.length; i++) {
                if (!predicate.test((E) this.elements[i])) {
                    return false;
                }
            }

            return true;
        }

    }

    static class HashArray16BackedSet<E> extends AbstractImmutableSet<E> {

        private static final int TABLE_SIZE = 16;
        private int size = 0;

        private final Object[] table1;
        private final Object[] table2;
        private Object[] flat;

        HashArray16BackedSet(int size, Object[] table1, Object[] table2) {
            this.size = size;
            this.table1 = table1;
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
        public boolean contains(Object o) {
            return contains(o, hashPosition(o));
        }

        boolean contains(Object o, int pos) {
            if (o.equals(this.table1[pos])) {
                return true;
            } else if (this.table2 != null && o.equals(this.table2[pos])) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<E>() {

                private Object[] table = table1;
                private int i = findIndexOfNextNonNull(table1, 0);

                @Override
                public boolean hasNext() {
                    return table != null;
                }

                @Override
                public E next() {
                    if (table == null) {
                        throw new NoSuchElementException();
                    }

                    @SuppressWarnings("unchecked")
                    E element = (E) table[i];

                    i = findIndexOfNextNonNull(table, i + 1);

                    if (i == -1) {
                        if (table == table1) {
                            table = table2;

                            if (table != null) {
                                i = findIndexOfNextNonNull(table, 0);

                                if (i == -1) {
                                    table = null;
                                }
                            }
                        } else {
                            table = null;
                        }
                    }

                    return element;
                }
            };
        }

        @Override
        public Object[] toArray() {
            Object[] result = new Object[size];
            System.arraycopy(getFlatArray(), 0, result, 0, size);
            return result;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T[] toArray(T[] a) {
            T[] result = a.length >= size ? a : (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);

            System.arraycopy(getFlatArray(), 0, result, 0, size);

            return result;
        }

        @SuppressWarnings("unchecked")
        @Override
        E any() {
            return (E) findFirstNonNull(table1, table2);
        }

        @Override
        public E only() {
            if (size() != 1) {
                throw new IllegalStateException();
            }

            return any();
        }

        @SuppressWarnings("unchecked")
        @Override
        public ImmutableSet<E> matching(Predicate<E> predicate) {
            int table1count = 0;
            int table2count = 0;

            Object[] newTable1 = new Object[TABLE_SIZE];
            Object[] newTable2 = table2 != null ? new Object[TABLE_SIZE] : null;

            for (int i = 0; i < TABLE_SIZE; i++) {
                Object v = this.table1[i];

                if (v != null) {
                    if (predicate.test((E) v)) {
                        newTable1[i] = v;
                        table1count++;
                    }
                }
            }

            if (this.table2 != null) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    Object v = this.table2[i];

                    if (v != null) {
                        if (predicate.test((E) v)) {
                            newTable2[i] = v;
                            table2count++;
                        }
                    }
                }
            }

            int count = table1count + table2count;

            if (count == 0) {
                return empty();
            } else if (count == 1) {
                return new OneElementSet<E>((E) findFirstNonNull(newTable1, newTable2));
            } else if (count < size) {
                if (table2count == 0) {
                    return new HashArray16BackedSet<E>(count, newTable1, null);
                } else if (table1count == 0) {
                    return new HashArray16BackedSet<E>(count, newTable2, null);
                } else {
                    return new HashArray16BackedSet<E>(count, newTable1, newTable2);
                }
            } else {
                return this;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public ImmutableSet<E> intersection(Set<E> other) {
            if (other.isEmpty()) {
                return empty();
            }

            if (other == this) {
                return this;
            }

            if (other instanceof ImmutableSet && other.size() < this.size()) {
                return ((ImmutableSet<E>) other).intersection(this);
            }

            if (other instanceof HashArray16BackedSet) {
                return intersection((HashArray16BackedSet<E>) other);
            }

            int table1count = 0;
            int table2count = 0;

            Object[] newTable1 = new Object[TABLE_SIZE];
            Object[] newTable2 = table2 != null ? new Object[TABLE_SIZE] : null;

            for (int i = 0; i < TABLE_SIZE; i++) {
                Object v = this.table1[i];

                if (v != null) {
                    if (other.contains((E) v)) {
                        newTable1[i] = v;
                        table1count++;
                    }

                }
            }

            if (this.table2 != null) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    Object v = this.table2[i];

                    if (v != null) {
                        if (other.contains((E) v)) {
                            newTable2[i] = v;
                            table2count++;
                        }
                    }
                }
            }

            int count = table1count + table2count;

            if (count == 0) {
                return empty();
            } else if (count == 1) {
                return new OneElementSet<E>((E) findFirstNonNull(newTable1, newTable2));
            } else if (count < size) {
                if (table2count == 0) {
                    return new HashArray16BackedSet<E>(count, newTable1, null);
                } else if (table1count == 0) {
                    return new HashArray16BackedSet<E>(count, newTable2, null);
                } else {
                    return new HashArray16BackedSet<E>(count, newTable1, newTable2);
                }
            } else {
                return this;
            }
        }

        @SuppressWarnings("unchecked")
        private ImmutableSet<E> intersection(HashArray16BackedSet<E> other) {
            int k = 0;

            Object[] newTable1 = new Object[TABLE_SIZE];
            Object[] newTable2 = table2 != null ? new Object[TABLE_SIZE] : null;

            for (int i = 0; i < TABLE_SIZE; i++) {
                Object v = this.table1[i];

                if (v != null) {
                    if (other.table1[i] != null && other.table1[i].equals(v)) {
                        newTable1[i] = v;
                        k++;
                    } else if (other.table2 != null && other.table2[i] != null && other.table2[i].equals(v)) {
                        newTable1[i] = v;
                        k++;
                    }
                }
            }

            if (this.table2 != null) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    Object v = this.table2[i];

                    if (v != null) {
                        if (other.table1[i] != null && other.table1[i].equals(v)) {
                            newTable2[i] = v;
                            k++;
                        } else if (other.table2 != null && other.table2[i] != null && other.table2[i].equals(v)) {
                            newTable2[i] = v;
                            k++;
                        }
                    }
                }
            }

            if (k == 0) {
                return empty();
            } else if (k == 1) {
                E only = (E) findFirstNonNull(newTable1);

                if (only == null) {
                    only = (E) findFirstNonNull(newTable2);
                }

                return new OneElementSet<E>(only);
            } else if (k < size) {
                return new HashArray16BackedSet<E>(k, newTable1, newTable2);
            } else {
                return this;
            }
        }

        @Override
        public ImmutableSet<E> without(Collection<E> other) {
            if (other.isEmpty()) {
                return this;
            }

            return matching((e) -> !other.contains(e));
        }

        @Override
        public String toShortString() {
            StringBuilder result = new StringBuilder("[");
            Object[] flat = getFlatArray();

            for (int i = 0; i < flat.length; i++) {
                if (i != 0) {
                    result.append(",");
                }
                result.append(flat[i]);
            }

            result.append("]");

            return result.toString();
        }

        @Override
        public ImmutableSet<E> with(E other) {

            int pos = hashPosition(other);

            if (this.table1[pos] != null) {
                if (other.equals(this.table1[pos])) {
                    // already contained
                    return this;
                } else if (this.table2 != null) {
                    if (this.table2[pos] != null) {
                        if (other.equals(this.table2[pos])) {
                            // already contained
                            return this;
                        } else {
                            return new WithSet<>(this, other);
                        }
                    } else {
                        Object[] newTable2 = this.table2.clone();
                        newTable2[pos] = other;
                        return new HashArray16BackedSet<>(size + 1, this.table1, newTable2);
                    }
                } else {
                    Object[] newTable2 = new Object[TABLE_SIZE];
                    newTable2[pos] = other;
                    return new HashArray16BackedSet<>(size + 1, this.table1, newTable2);
                }
            } else {
                Object[] newTable1 = this.table1.clone();
                newTable1[pos] = other;
                return new HashArray16BackedSet<>(size + 1, newTable1, this.table2);
            }
        }

        @Override
        public ImmutableSet<E> with(ImmutableSet<E> other) {
            int otherSize = other.size();

            if (otherSize == 0) {
                return this;
            } else if (otherSize == 1) {
                return this.with(other.only());
            } else {
                HashArray16BackedSet.Builder<E> builder = new HashArray16BackedSet.Builder<E>(this);
                builder.with(other);
                return builder.build();
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public ImmutableSet<E> with(E... other) {
            int otherSize = other.length;

            if (otherSize == 0) {
                return this;
            } else if (otherSize == 1) {
                return this.with(other[0]);
            } else {
                InternalBuilder<E> builder = new HashArray16BackedSet.Builder<E>(this);

                for (int i = 0; i < other.length; i++) {
                    builder = builder.with(other[i]);
                }

                return builder.build();
            }
        }

        @Override
        public ImmutableSet<E> without(E other) {
            int pos = hashPosition(other);

            if (this.table1[pos] != null && other.equals(this.table1[pos])) {
                if (size == 1) {
                    return empty();
                }

                Object[] newTable1 = this.table1.clone();
                newTable1[pos] = null;
                return new HashArray16BackedSet<>(size - 1, newTable1, this.table2);
            } else if (this.table2 != null && this.table2[pos] != null && other.equals(this.table1[pos])) {
                if (size == 1) {
                    return empty();
                }

                Object[] newTable2 = this.table2.clone();
                newTable2[pos] = null;
                return new HashArray16BackedSet<>(size - 1, this.table1, newTable2);
            } else {
                return this;
            }
        }

        Object[] getFlatArray() {
            if (flat != null) {
                return flat;
            }

            Object[] flat = new Object[size];
            int k = 0;

            for (int i = 0; i < table1.length; i++) {
                Object v = table1[i];

                if (v != null) {
                    flat[k] = v;
                    k++;
                }
            }

            if (table2 != null) {
                for (int i = 0; i < table2.length; i++) {
                    Object v = table2[i];

                    if (v != null) {
                        flat[k] = v;
                        k++;
                    }
                }
            }

            this.flat = flat;

            return flat;
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
                throw new IllegalArgumentException("ImmutableSet does not support null values");
            }

            int hash = e.hashCode();
            int pos = (hash & 0xf) ^ (hash >> 4 & 0xf) ^ (hash >> 8 & 0xf) ^ (hash >> 12 & 0xf) ^ (hash >> 16 & 0xf) ^ (hash >> 20 & 0xf)
                    ^ (hash >> 24 & 0xf) ^ (hash >> 28 & 0xf);
            return pos;
        }

        static class Builder<E> extends InternalBuilder<E> {
            private E first;
            private Object[] tail1;
            private Object[] tail2;
            private int size = 0;

            public Builder() {

            }

            @SuppressWarnings("unchecked")
            public Builder(HashArray16BackedSet<E> initialContent) {
                this.tail1 = initialContent.table1.clone();
                this.tail2 = initialContent.table2 != null ? initialContent.table2.clone() : null;
                this.size = initialContent.size;

                int firstIndex = findIndexOfNextNonNull(tail1, size);

                if (firstIndex != -1) {
                    this.first = (E) this.tail1[firstIndex];
                    this.tail1[firstIndex] = null;
                }
            }

            public InternalBuilder<E> with(E e) {
                if (e == null) {
                    throw new IllegalArgumentException("Null elements are not supported");
                }

                if (first == null) {
                    first = e;
                    size++;
                    return this;
                } else if (first.equals(e)) {
                    // done
                    return this;
                } else if (tail1 == null) {
                    tail1 = new Object[TABLE_SIZE];
                    tail1[hashPosition(e)] = e;
                    size++;
                    return this;
                } else {
                    int position = hashPosition(e);

                    if (tail1[position] == null) {
                        tail1[position] = e;
                        size++;
                        return this;
                    } else if (tail1[position].equals(e)) {
                        // done
                        return this;
                    } else {
                        // collision

                        if (tail2 == null) {
                            tail2 = new Object[TABLE_SIZE];
                            tail2[position] = e;
                            size++;
                            return this;
                        } else if (tail2[position] == null) {
                            tail2[position] = e;
                            size++;
                            return this;
                        } else if (tail2[position].equals(e)) {
                            // done     
                            return this;
                        } else {
                            // collision

                            return new SetBackedSet.Builder<>(build()).with(e);
                        }
                    }
                }
            }

            @Override
            InternalBuilder<E> with(Collection<E> collection) {
                InternalBuilder<E> builder = this;

                for (E e : collection) {
                    builder = builder.with(e);
                }

                return builder;
            }

            @SuppressWarnings("unchecked")
            public ImmutableSet<E> build() {
                if (size == 0) {
                    return ImmutableSet.empty();
                } else if (first == null) {
                    return new HashArray16BackedSet<>(size, tail1, tail2);
                } else if (size == 1) {
                    return new OneElementSet<>(first);
                } else if (size == 2) {
                    return new TwoElementSet<>(first, (E) findFirstNonNull(this.tail1));
                } else {
                    // We need to integrate the first element
                    int firstPos = hashPosition(first);

                    if (this.tail1[firstPos] == null) {
                        this.tail1[firstPos] = first;
                        return new HashArray16BackedSet<>(size, tail1, tail2);
                    } else if (this.tail2 == null) {
                        this.tail2 = new Object[TABLE_SIZE];
                        this.tail2[firstPos] = first;
                        return new HashArray16BackedSet<>(size, tail1, tail2);
                    } else if (this.tail2[firstPos] == null) {
                        this.tail2[firstPos] = first;
                        return new HashArray16BackedSet<>(size, tail1, tail2);
                    } else {
                        return new WithSet<>(new HashArray16BackedSet<>(size, tail1, tail2), first);
                    }

                }
            }

            @Override
            int size() {
                return size;
            }

            @Override
            boolean remove(E e) {
                if (e.equals(first)) {
                    first = null;
                    size--;

                    return true;
                } else {
                    int position = hashPosition(e);

                    if (tail1[position] != null && tail1[position].equals(e)) {
                        tail1[position] = null;
                        size--;
                        return true;
                    } else if (tail2 != null && tail2[position] != null && tail2[position].equals(e)) {
                        tail2[position] = null;
                        size--;
                        return true;
                    }
                }

                return false;
            }

            @Override
            void clear() {
                size = 0;
                first = null;

                if (tail1 != null) {
                    Arrays.fill(tail1, null);
                }

                tail2 = null;
            }

            @Override
            boolean contains(E e) {
                if (e.equals(first)) {
                    return true;
                } else {
                    int position = hashPosition(e);

                    if (tail1[position] != null && tail1[position].equals(e)) {
                        return true;
                    } else if (tail2 != null && tail2[position] != null && tail2[position].equals(e)) {
                        return true;
                    }
                }

                return false;
            }

            @Override
            boolean containsAny(Set<E> set) {
                if (set instanceof HashArray16BackedSet) {
                    return containsAny((HashArray16BackedSet<E>) set);
                } else {
                    for (E e : set) {
                        if (contains(e)) {
                            return true;
                        }
                    }
                }

                return false;
            }

            private boolean containsAny(HashArray16BackedSet<E> set) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    if (set.table1[i] != null) {
                        if (contains(set.table1[i], i)) {
                            return true;
                        }
                    }

                    if (set.table2 != null && set.table2[i] != null) {
                        if (contains(set.table2[i], i)) {
                            return true;
                        }
                    }
                }

                return false;
            }

            @Override
            boolean containsAll(Set<E> set) {
                if (set instanceof HashArray16BackedSet) {
                    return containsAll((HashArray16BackedSet<E>) set);
                } else {
                    for (E e : set) {
                        if (!contains(e)) {
                            return false;
                        }
                    }
                    return true;
                }
            }

            private boolean containsAll(HashArray16BackedSet<E> set) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    if (set.table1[i] != null) {
                        if (!contains(set.table1[i], i)) {
                            return false;
                        }
                    }

                    if (set.table2 != null && set.table2[i] != null) {
                        if (!contains(set.table2[i], i)) {
                            return false;
                        }
                    }
                }

                return true;
            }

            private boolean contains(Object e, int hashPosition) {
                if (tail1 != null && tail1[hashPosition] != null && tail1[hashPosition].equals(e)) {
                    return true;
                } else if (tail2 != null && tail2[hashPosition] != null && tail2[hashPosition].equals(e)) {
                    return true;
                } else if (first.equals(e)) {
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            Iterator<E> iterator() {
                if (size == 0) {
                    return Collections.emptyIterator();
                }

                return new Iterator<E>() {

                    int currentState = -1;
                    int nextState = 0;
                    int currentPos = 0;
                    int nextPos = 0;
                    E current;
                    E prev;
                    int prevState;
                    int prevPos;

                    @Override
                    public boolean hasNext() {
                        if (current == null) {
                            initNext();
                        }
                        return current != null;
                    }

                    @Override
                    public E next() {
                        if (current != null) {
                            E result = current;
                            prev = current;
                            prevState = currentState;
                            prevPos = currentPos;
                            current = null;
                            return result;
                        } else {
                            throw new NoSuchElementException();
                        }
                    }

                    @Override
                    public void remove() {
                        if (prev == null) {
                            throw new NoSuchElementException();
                        }

                        if (prevState == 0) {
                            if (prev.equals(first)) {
                                first = null;
                                size--;
                            }
                        } else if (prevState == 1) {
                            if (tail1 != null && prev.equals(tail1[this.prevPos])) {
                                tail1[this.prevPos] = null;
                                size--;
                            }
                        } else if (prevState == 2) {
                            if (tail2 != null && prev.equals(tail2[this.prevPos])) {
                                tail2[this.prevPos] = null;
                                size--;
                            }
                        }

                    }

                    @SuppressWarnings("unchecked")
                    private void initNext() {
                        if (nextState == 0) {
                            currentState = 0;
                            nextState = 1;
                            if (first != null) {
                                current = first;
                                return;
                            }
                        }

                        if (nextState == 1) {
                            if (tail1 != null) {
                                int nextIndex = findIndexOfNextNonNull(tail1, nextPos);

                                if (nextIndex != -1) {
                                    this.currentState = 1;
                                    this.currentPos = nextIndex;
                                    this.nextPos = nextIndex + 1;
                                    this.current = (E) tail1[nextIndex];
                                    return;
                                } else {
                                    this.nextPos = 0;
                                    this.nextState = 2;
                                }
                            } else {
                                this.nextState = 2;
                            }
                        }

                        if (nextState == 2) {
                            if (tail2 != null) {
                                int nextIndex = findIndexOfNextNonNull(tail2, nextPos);

                                if (nextIndex != -1) {
                                    this.currentState = 2;
                                    this.currentPos = nextIndex;
                                    this.nextPos = nextIndex + 1;
                                    this.current = (E) tail2[nextIndex];
                                    return;
                                } else {
                                    this.nextPos = 0;
                                    this.nextState = 2;
                                }
                            } else {
                                this.nextState = 3;
                            }
                        }

                        this.currentState = 3;
                        this.current = null;
                    }

                };
            }

            @SuppressWarnings("unchecked")
            @Override
            E any() {
                if (first != null) {
                    return first;
                }

                return (E) findFirstNonNull(this.tail1, this.tail2);
            }

        }
    }

    static class HashArray64BackedSet<E> extends AbstractImmutableSet<E> {

        private static final int TABLE_SIZE = 64;
        private int size = 0;

        private final Object[] table1;
        private final Object[] table2;
        private Object[] flat;

        HashArray64BackedSet(int size, Object[] table1, Object[] table2) {
            this.size = size;
            this.table1 = table1;
            this.table2 = table2;
        }

        HashArray64BackedSet(int size, Object[] table1, Object[] table2, Object[] flat) {
            this.size = size;
            this.table1 = table1;
            this.table2 = table2;
            this.flat = flat;
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
        public boolean contains(Object o) {
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
        public Iterator<E> iterator() {
            return new Iterator<E>() {

                private Object[] table = table1;
                private int i = findIndexOfNextNonNull(table1, 0);

                @Override
                public boolean hasNext() {
                    return table != null;
                }

                @Override
                public E next() {
                    if (table == null) {
                        throw new NoSuchElementException();
                    }

                    @SuppressWarnings("unchecked")
                    E element = (E) table[i];

                    i = findIndexOfNextNonNull(table, i + 1);

                    if (i == -1) {
                        if (table == table1) {
                            table = table2;

                            if (table != null) {
                                i = findIndexOfNextNonNull(table, 0);

                                if (i == -1) {
                                    table = null;
                                }
                            }
                        } else {
                            table = null;
                        }
                    }

                    return element;
                }
            };
        }

        @Override
        public Object[] toArray() {
            Object[] result = new Object[size];
            System.arraycopy(getFlatArray(), 0, result, 0, size);
            return result;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T[] toArray(T[] a) {
            T[] result = a.length >= size ? a : (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);

            System.arraycopy(getFlatArray(), 0, result, 0, size);

            return result;
        }

        @SuppressWarnings("unchecked")
        @Override
        E any() {
            return (E) getFlatArray()[0];
        }

        @Override
        public E only() {
            if (size() != 1) {
                throw new IllegalStateException();
            }

            return any();
        }

        @SuppressWarnings("unchecked")
        @Override
        public ImmutableSet<E> matching(Predicate<E> predicate) {
            int table1count = 0;
            int table2count = 0;

            Object[] newTable1 = new Object[TABLE_SIZE];
            Object[] newTable2 = table2 != null ? new Object[TABLE_SIZE] : null;
            Object[] newFlat = new Object[size];

            for (int i = 0; i < TABLE_SIZE; i++) {
                Object v = this.table1[i];

                if (v != null) {
                    if (predicate.test((E) v)) {
                        newTable1[i] = v;
                        newFlat[table1count] = v;
                        table1count++;
                    }
                }
            }

            int count = table1count;

            if (this.table2 != null) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    Object v = this.table2[i];

                    if (v != null) {
                        if (predicate.test((E) v)) {
                            newTable2[i] = v;
                            newFlat[count] = v;
                            table2count++;
                            count++;
                        }
                    }
                }
            }

            if (count == 0) {
                return empty();
            } else if (count == 1) {
                return new OneElementSet<E>((E) newFlat[0]);
            } else if (count == 2) {
                return new TwoElementSet<E>((E) newFlat[0], (E) newFlat[1]);
            } else if (count < size) {
                if (table2count == 0) {
                    return new HashArray64BackedSet<E>(count, newTable1, null, newFlat);
                } else if (table1count == 0) {
                    return new HashArray64BackedSet<E>(count, newTable2, null, newFlat);
                } else {
                    return new HashArray64BackedSet<E>(count, newTable1, newTable2, newFlat);
                }
            } else {
                return this;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public ImmutableSet<E> intersection(Set<E> other) {
            if (other.isEmpty()) {
                return empty();
            }

            if (other == this) {
                return this;
            }

            if (other instanceof ImmutableSet && other.size() < this.size()) {
                return ((ImmutableSet<E>) other).intersection(this);
            }

            if (other instanceof HashArray64BackedSet) {
                return intersection((HashArray64BackedSet<E>) other);
            }

            int table1count = 0;
            int table2count = 0;

            Object[] newTable1 = new Object[TABLE_SIZE];
            Object[] newTable2 = table2 != null ? new Object[TABLE_SIZE] : null;
            Object[] newFlat = new Object[size];

            for (int i = 0; i < TABLE_SIZE; i++) {
                Object v = this.table1[i];

                if (v != null) {
                    if (other.contains((E) v)) {
                        newTable1[i] = v;
                        newFlat[table1count] = v;
                        table1count++;
                    }

                }
            }

            int count = table1count;

            if (this.table2 != null) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    Object v = this.table2[i];

                    if (v != null) {
                        if (other.contains((E) v)) {
                            newTable2[i] = v;
                            newFlat[count] = v;
                            table2count++;
                            count++;
                        }
                    }
                }
            }

            if (count == 0) {
                return empty();
            } else if (count == 1) {
                return new OneElementSet<E>((E) newFlat[0]);
            } else if (count == 2) {
                return new TwoElementSet<E>((E) newFlat[0], (E) newFlat[1]);
            } else if (count < size) {
                if (table2count == 0) {
                    return new HashArray64BackedSet<E>(count, newTable1, null, newFlat);
                } else if (table1count == 0) {
                    return new HashArray64BackedSet<E>(count, newTable2, null, newFlat);
                } else {
                    return new HashArray64BackedSet<E>(count, newTable1, newTable2, newFlat);
                }
            } else {
                return this;
            }
        }

        @SuppressWarnings("unchecked")
        private ImmutableSet<E> intersection(HashArray64BackedSet<E> other) {
            int table1count = 0;
            int table2count = 0;

            Object[] newTable1 = new Object[TABLE_SIZE];
            Object[] newTable2 = table2 != null ? new Object[TABLE_SIZE] : null;

            for (int i = 0; i < TABLE_SIZE; i++) {
                Object v = this.table1[i];

                if (v != null) {
                    if ((other.table1[i] != null && other.table1[i].equals(v))
                            || (other.table2 != null && other.table2[i] != null && other.table2[i].equals(v))) {
                        newTable1[i] = v;
                        table1count++;
                    }
                }
            }

            if (this.table2 != null) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    Object v = this.table2[i];

                    if (v != null) {
                        if ((other.table1[i] != null && other.table1[i].equals(v))
                                || (other.table2 != null && other.table2[i] != null && other.table2[i].equals(v))) {
                            if (newTable1[i] == null) {
                                newTable1[i] = v;
                                table1count++;
                            } else {
                                newTable2[i] = v;
                                table2count++;
                            }
                        }
                    }
                }
            }

            int count = table1count + table2count;

            if (count == 0) {
                return empty();
            } else if (count == 1) {
                return new OneElementSet<E>((E) findFirstNonNull(newTable1, newTable2));
            } else if (count < size) {
                if (table2count == 0) {
                    return new HashArray64BackedSet<E>(count, newTable1, null);
                } else if (table1count == 0) {
                    return new HashArray64BackedSet<E>(count, newTable2, null);
                } else {
                    return new HashArray64BackedSet<E>(count, newTable1, newTable2);
                }
            } else {
                return this;
            }
        }

        @Override
        public ImmutableSet<E> without(Collection<E> other) {
            if (other.isEmpty()) {
                return this;
            }

            return matching((e) -> !other.contains(e));
        }

        @Override
        public String toShortString() {
            StringBuilder result = new StringBuilder("[");
            Object[] flat = getFlatArray();

            for (int i = 0; i < flat.length; i++) {
                if (i != 0) {
                    result.append(",");
                }
                result.append(flat[i]);
            }

            result.append("]");

            return result.toString();
        }

        @Override
        public ImmutableSet<E> with(E other) {

            int pos = hashPosition(other);

            if (this.table1[pos] != null) {
                if (other.equals(this.table1[pos])) {
                    // already contained
                    return this;
                } else if (this.table2 != null) {
                    if (this.table2[pos] != null) {
                        if (other.equals(this.table2[pos])) {
                            // already contained
                            return this;
                        } else {
                            return new WithSet<>(this, other);
                        }
                    } else {
                        Object[] newTable2 = this.table2.clone();
                        newTable2[pos] = other;
                        return new HashArray64BackedSet<>(size + 1, this.table1, newTable2);
                    }
                } else {
                    Object[] newTable2 = new Object[TABLE_SIZE];
                    newTable2[pos] = other;
                    return new HashArray64BackedSet<>(size + 1, this.table1, newTable2);
                }
            } else {
                Object[] newTable1 = this.table1.clone();
                newTable1[pos] = other;
                return new HashArray64BackedSet<>(size + 1, newTable1, this.table2);
            }
        }

        @Override
        public ImmutableSet<E> with(ImmutableSet<E> other) {
            int otherSize = other.size();

            if (otherSize == 0) {
                return this;
            } else if (otherSize == 1) {
                return this.with(other.only());
            } else {
                HashArray64BackedSet.Builder<E> builder = new HashArray64BackedSet.Builder<E>(this);
                builder.with(other);
                return builder.build();
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public ImmutableSet<E> with(E... other) {
            int otherSize = other.length;

            if (otherSize == 0) {
                return this;
            } else if (otherSize == 1) {
                return this.with(other[0]);
            } else {
                InternalBuilder<E> builder = new HashArray64BackedSet.Builder<E>(this);

                for (int i = 0; i < other.length; i++) {
                    builder = builder.with(other[i]);
                }

                return builder.build();
            }
        }

        @Override
        public ImmutableSet<E> without(E other) {
            int pos = hashPosition(other);

            if (this.table1[pos] != null && other.equals(this.table1[pos])) {
                if (size == 1) {
                    return empty();
                }

                Object[] newTable1 = this.table1.clone();
                newTable1[pos] = null;
                return new HashArray64BackedSet<>(size - 1, newTable1, this.table2);
            } else if (this.table2 != null && this.table2[pos] != null && other.equals(this.table1[pos])) {
                if (size == 1) {
                    return empty();
                }

                Object[] newTable2 = this.table2.clone();
                newTable2[pos] = null;
                return new HashArray64BackedSet<>(size - 1, this.table1, newTable2);
            } else {
                return this;
            }
        }

        Object[] getFlatArray() {
            if (flat != null) {
                return flat;
            }

            Object[] flat = new Object[size];
            int k = 0;

            for (int i = 0; i < table1.length; i++) {
                Object v = table1[i];

                if (v != null) {
                    flat[k] = v;
                    k++;
                }
            }

            if (table2 != null) {
                for (int i = 0; i < table2.length; i++) {
                    Object v = table2[i];

                    if (v != null) {
                        flat[k] = v;
                        k++;
                    }
                }
            }

            this.flat = flat;

            return flat;
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
                throw new IllegalArgumentException("ImmutableSet does not support null values");
            }

            int hash = e.hashCode();
            int pos = (hash & 0x3f) ^ (hash >> 6 & 0x3f) ^ (hash >> 12 & 0x3f) ^ (hash >> 18 & 0x3f) ^ (hash >> 24 & 0xf) ^ (hash >> 28 & 0xf);
            return pos;
        }

        static class Builder<E> extends InternalBuilder<E> {
            private E first;
            private Object[] tail1;
            private Object[] tail2;
            private int size = 0;

            public Builder() {

            }

            @SuppressWarnings("unchecked")
            public Builder(HashArray64BackedSet<E> initialContent) {
                this.tail1 = initialContent.table1.clone();
                this.tail2 = initialContent.table2 != null ? initialContent.table2.clone() : null;
                this.size = initialContent.size;

                int firstIndex = findIndexOfNextNonNull(tail1, size);

                if (firstIndex != -1) {
                    this.first = (E) this.tail1[firstIndex];
                    this.tail1[firstIndex] = null;
                }
            }

            public InternalBuilder<E> with(E e) {
                if (e == null) {
                    throw new IllegalArgumentException("Null elements are not supported");
                }

                if (first == null) {
                    first = e;
                    size++;
                    return this;
                } else if (first.equals(e)) {
                    // done
                    return this;
                } else if (tail1 == null) {
                    tail1 = new Object[TABLE_SIZE];
                    tail1[hashPosition(e)] = e;
                    size++;
                    return this;
                } else {
                    int position = hashPosition(e);

                    if (tail1[position] == null) {
                        tail1[position] = e;
                        size++;
                        return this;
                    } else if (tail1[position].equals(e)) {
                        // done
                        return this;
                    } else {
                        // collision

                        if (tail2 == null) {
                            tail2 = new Object[TABLE_SIZE];
                            tail2[position] = e;
                            size++;
                            return this;
                        } else if (tail2[position] == null) {
                            tail2[position] = e;
                            size++;
                            return this;
                        } else if (tail2[position].equals(e)) {
                            // done     
                            return this;
                        } else {
                            // collision

                            return new SetBackedSet.Builder<>(build()).with(e);
                        }
                    }
                }
            }

            @Override
            InternalBuilder<E> with(Collection<E> collection) {
                InternalBuilder<E> builder = this;

                for (E e : collection) {
                    builder = builder.with(e);
                }

                return builder;
            }

            @SuppressWarnings("unchecked")
            public ImmutableSet<E> build() {
                if (size == 0) {
                    return ImmutableSet.empty();
                } else if (first == null) {
                    return new HashArray64BackedSet<>(size, tail1, tail2);
                } else if (size == 1) {
                    return new OneElementSet<>(first);
                } else if (size == 2) {
                    return new TwoElementSet<>(first, (E) findFirstNonNull(this.tail1));
                } else {
                    // We need to integrate the first element
                    int firstPos = hashPosition(first);

                    if (this.tail1[firstPos] == null) {
                        this.tail1[firstPos] = first;
                        return new HashArray64BackedSet<>(size, tail1, tail2);
                    } else if (this.tail2 == null) {
                        this.tail2 = new Object[TABLE_SIZE];
                        this.tail2[firstPos] = first;
                        return new HashArray64BackedSet<>(size, tail1, tail2);
                    } else if (this.tail2[firstPos] == null) {
                        this.tail2[firstPos] = first;
                        return new HashArray64BackedSet<>(size, tail1, tail2);
                    } else {
                        return new WithSet<>(new HashArray64BackedSet<>(size, tail1, tail2), first);
                    }

                }
            }

            @Override
            int size() {
                return size;
            }

            @Override
            boolean remove(E e) {
                if (e.equals(first)) {
                    first = null;
                    size--;

                    return true;
                } else {
                    int position = hashPosition(e);

                    if (tail1[position] != null && tail1[position].equals(e)) {
                        tail1[position] = null;
                        size--;
                        return true;
                    } else if (tail2 != null && tail2[position] != null && tail2[position].equals(e)) {
                        tail2[position] = null;
                        size--;
                        return true;
                    }
                }

                return false;
            }

            @Override
            void clear() {
                size = 0;
                first = null;

                if (tail1 != null) {
                    Arrays.fill(tail1, null);
                }

                tail2 = null;
            }

            @Override
            boolean contains(E e) {
                if (e.equals(first)) {
                    return true;
                } else {
                    int position = hashPosition(e);

                    if (tail1[position] != null && tail1[position].equals(e)) {
                        return true;
                    } else if (tail2 != null && tail2[position] != null && tail2[position].equals(e)) {
                        return true;
                    }
                }

                return false;
            }

            @Override
            boolean containsAny(Set<E> set) {
                if (set instanceof HashArray64BackedSet) {
                    return containsAny((HashArray64BackedSet<E>) set);
                } else {
                    for (E e : set) {
                        if (contains(e)) {
                            return true;
                        }
                    }
                }

                return false;
            }

            private boolean containsAny(HashArray64BackedSet<E> set) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    if (set.table1[i] != null) {
                        if (contains(set.table1[i], i)) {
                            return true;
                        }
                    }

                    if (set.table2 != null && set.table2[i] != null) {
                        if (contains(set.table2[i], i)) {
                            return true;
                        }
                    }
                }

                return false;
            }

            @Override
            boolean containsAll(Set<E> set) {
                if (set instanceof HashArray64BackedSet) {
                    return containsAll((HashArray64BackedSet<E>) set);
                } else {
                    for (E e : set) {
                        if (!contains(e)) {
                            return false;
                        }
                    }

                    return true;
                }
            }

            private boolean containsAll(HashArray64BackedSet<E> set) {
                for (int i = 0; i < TABLE_SIZE; i++) {
                    if (set.table1[i] != null) {
                        if (!contains(set.table1[i], i)) {
                            return false;
                        }
                    }

                    if (set.table2 != null && set.table2[i] != null) {
                        if (!contains(set.table2[i], i)) {
                            return false;
                        }
                    }
                }

                return true;
            }

            private boolean contains(Object e, int hashPosition) {
                if (tail1 != null && tail1[hashPosition] != null && tail1[hashPosition].equals(e)) {
                    return true;
                } else if (tail2 != null && tail2[hashPosition] != null && tail2[hashPosition].equals(e)) {
                    return true;
                } else if (first.equals(e)) {
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            Iterator<E> iterator() {
                if (size == 0) {
                    return Collections.emptyIterator();
                }

                return new Iterator<E>() {

                    int state = 0;
                    int nextPos = 0;
                    int nextNextPos = 0;
                    E next;
                    E prev;
                    int prevState;
                    int prevPos;

                    @Override
                    public boolean hasNext() {
                        if (next == null) {
                            initNext();
                        }
                        return next != null;
                    }

                    @Override
                    public E next() {
                        if (next != null) {
                            E result = next;
                            prev = next;
                            prevState = state;
                            prevPos = nextPos;
                            next = null;
                            return result;
                        } else {
                            throw new NoSuchElementException();
                        }
                    }

                    @Override
                    public void remove() {
                        if (prev == null) {
                            throw new NoSuchElementException();
                        }

                        if (prevState == 0) {
                            if (prev.equals(first)) {
                                first = null;
                                size--;
                            }
                        } else if (prevState == 1) {
                            if (tail1 != null && prev.equals(tail1[this.prevPos])) {
                                tail1[this.prevPos] = null;
                                size--;
                            }
                        } else if (prevState == 2) {
                            if (tail2 != null && prev.equals(tail2[this.prevPos])) {
                                tail2[this.prevPos] = null;
                                size--;
                            }
                        }

                    }

                    @SuppressWarnings("unchecked")
                    private void initNext() {
                        if (state == 0) {
                            state = 1;
                            if (first != null) {
                                next = first;
                                return;
                            }
                        }

                        if (state == 1) {
                            if (tail1 != null) {
                                int nextIndex = findIndexOfNextNonNull(tail1, nextNextPos);

                                if (nextIndex != -1) {
                                    this.nextPos = nextIndex;
                                    this.nextNextPos = nextIndex + 1;
                                    this.next = (E) tail1[nextIndex];
                                    return;
                                } else {
                                    this.nextNextPos = 0;
                                    this.state = 2;
                                }
                            } else {
                                this.state = 2;
                            }
                        }

                        if (state == 2) {
                            if (tail2 != null) {
                                int nextIndex = findIndexOfNextNonNull(tail2, nextNextPos);

                                if (nextIndex != -1) {
                                    this.nextPos = nextIndex;
                                    this.nextNextPos = nextIndex + 1;
                                    this.next = (E) tail2[nextIndex];
                                    return;
                                } else {
                                    this.nextNextPos = 0;
                                    this.state = 2;
                                }
                            } else {
                                this.state = 3;
                            }
                        }

                        this.next = null;
                    }

                };
            }

            @SuppressWarnings("unchecked")
            @Override
            E any() {
                if (first != null) {
                    return first;
                }

                return (E) findFirstNonNull(tail1, tail2);
            }

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

        @Override
        public ImmutableSet<E> matching(Predicate<E> predicate) {
            Set<E> newSet = new HashSet<>(this.elements.size());

            for (E e : this.elements) {
                if (predicate.test(e)) {
                    newSet.add(e);
                }
            }

            return of(newSet);
        }

        @Override
        public ImmutableSet<E> intersection(Set<E> other) {
            if (other.isEmpty()) {
                return empty();
            }

            if (other instanceof ImmutableSet && other.size() < this.size()) {
                return ((ImmutableSet<E>) other).intersection(this);
            }

            Set<E> newSet = new HashSet<>(this.elements);

            newSet.retainAll(other);

            return of(newSet);
        }

        @Override
        public ImmutableSet<E> without(Collection<E> other) {
            if (other.isEmpty()) {
                return this;
            }

            return matching((e) -> !other.contains(e));
        }

        @Override
        public String toShortString() {
            int i = 0;
            StringBuilder result = new StringBuilder("[");

            for (E e : this.elements) {
                if (i != 0) {
                    result.append(", ");
                }
                result.append(e);
                i++;

                if (i >= 7 && i < this.elements.size() - 1) {
                    result.append(", ").append(this.elements.size() - i).append(" more...");
                    break;
                }
            }

            result.append("]");

            return result.toString();
        }

        static class Builder<E> extends InternalBuilder<E> {
            private HashSet<E> delegate;

            Builder(int expectedCapacity) {
                this.delegate = new HashSet<>(expectedCapacity);
            }

            Builder(Collection<E> set) {
                this.delegate = new HashSet<>(set);
            }

            public Builder<E> with(E e) {
                this.delegate.add(e);
                return this;
            }

            @Override
            ImmutableSet<E> build() {
                return new SetBackedSet<>(this.delegate);
            }

            @Override
            InternalBuilder<E> with(Collection<E> e) {
                this.delegate.addAll(e);
                return this;
            }

            @Override
            int size() {
                return delegate.size();
            }

            @Override
            boolean remove(E e) {
                return delegate.remove(e);
            }

            @Override
            boolean contains(E e) {
                return delegate.contains(e);
            }

            @Override
            boolean containsAny(Set<E> set) {
                for (E e : set) {
                    if (delegate.contains(e)) {
                        return true;
                    }
                }

                return false;
            }

            @Override
            boolean containsAll(Set<E> set) {
                return delegate.containsAll(set);
            }

            @Override
            Iterator<E> iterator() {
                return delegate.iterator();
            }

            @Override
            E any() {
                return delegate.iterator().next();
            }

            @Override
            void clear() {
                delegate.clear();
            }

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
        public E only() {
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

        @Override
        public ImmutableSet<E> matching(Predicate<E> predicate) {
            return this;
        }

        @Override
        public ImmutableSet<E> intersection(Set<E> other) {
            return this;
        }

        @Override
        public ImmutableSet<E> without(Collection<E> other) {
            return this;
        }

        @Override
        public String toString() {
            return "[]";
        }

        @Override
        public String toShortString() {
            return "[]";
        }

    }

    static class WithSet<E> extends AbstractImmutableSet<E> {

        private final ImmutableSet<E> base;
        private final E additional;
        private final int size;

        WithSet(ImmutableSet<E> base, E additional) {
            this.base = base;
            this.additional = additional;
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
        public boolean contains(Object o) {
            return Objects.equals(o, additional) || this.base.contains(o);
        }

        @Override
        E any() {
            return additional;
        }

        @Override
        public E only() {
            if (size == 1) {
                return additional;
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<E>() {

                private Iterator<E> delegate = base.iterator();
                private boolean hasAdditional = true;

                @Override
                public boolean hasNext() {
                    if (delegate.hasNext()) {
                        return true;
                    } else {
                        return hasAdditional;
                    }
                }

                @Override
                public E next() {
                    if (delegate.hasNext()) {
                        return delegate.next();
                    } else if (hasAdditional) {
                        hasAdditional = false;
                        return additional;
                    } else {
                        throw new NoSuchElementException();
                    }
                }
            };
        }

        @Override
        public int hashCode() {
            return base.hashCode() + additional.hashCode();
        }

        @Override
        public ImmutableSet<E> matching(Predicate<E> predicate) {
            ImmutableSet<E> baseResult = base.matching(predicate);

            if (predicate.test(additional)) {
                return baseResult.with(additional);
            } else {
                return baseResult;
            }
        }

        @Override
        public ImmutableSet<E> intersection(Set<E> other) {
            ImmutableSet<E> baseResult = base.intersection(other);

            if (other.contains(additional)) {
                return baseResult.with(additional);
            } else {
                return baseResult;
            }
        }

        @Override
        public ImmutableSet<E> without(Collection<E> other) {
            if (other.isEmpty()) {
                return this;
            }

            return matching((e) -> !other.contains(e));
        }

        @Override
        public String toShortString() {
            int i = 0;
            StringBuilder result = new StringBuilder("[");

            for (E e : this) {
                if (i != 0) {
                    result.append(", ");
                }
                result.append(e);
                i++;

                if (i >= 7 && i < size - 1) {
                    result.append(", ").append(size - i).append(" more...");
                    break;
                }
            }

            result.append("]");

            return result.toString();
        }

    }
}
