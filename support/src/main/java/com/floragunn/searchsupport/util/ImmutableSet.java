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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
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

    public static <E> ImmutableSet<E> ofNonNull(E e1, E e2) {
        if (e1 != null) {
            if (e2 != null) {
                return of(e1, e2);
            } else {
                return of(e1);
            }
        } else {
            if (e2 != null) {
                return of(e2);
            } else {
                return empty();
            }
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

    public static <E> ImmutableSet<E> flattenDeep(Collection<?> collection, Function<Object, E> mappingFunction) {
        if (collection.isEmpty()) {
            return ImmutableSet.empty();
        } else if (collection.size() == 1) {
            Object value = collection instanceof List ? ((List<?>) collection).get(0) : collection.iterator().next();

            if (value == null) {
                return ImmutableSet.empty();
            } else if (!(value instanceof Collection)) {
                return new OneElementSet<E>(mappingFunction.apply(value));
            }
        }

        ImmutableSet.Builder<E> result = new ImmutableSet.Builder<E>(collection.size());
        flatten(collection, result, mappingFunction);
        return result.build();
    }

    static <E> void flatten(Collection<?> collection, ImmutableSet.Builder<E> result, Function<Object, E> mappingFunction) {
        for (Object o : collection) {
            if (o instanceof Collection) {
                flatten((Collection<?>) o, result, mappingFunction);
            } else if (o != null) {
                E e = mappingFunction.apply(o);

                if (e != null) {
                    result.add(e);
                }
            }
        }
    }

    public static <C, E> ImmutableSet<E> map(Collection<C> collection, Function<C, E> mappingFunction) {
        ImmutableSet.Builder<E> builder = new ImmutableSet.Builder<>(collection.size());

        for (C c : collection) {
            E value = mappingFunction.apply(c);

            if (value != null) {
                builder.with(value);
            }
        }

        return builder.build();
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

    <O> ImmutableSet<O> map(Function<E, O> mappingFunction);

    <O> ImmutableSet<O> mapFlat(Function<E, Collection<O>> mappingFunction);

    boolean forAll(Predicate<E> predicate);

    boolean containsAny(Collection<E> e);

    E only();

    E any();

    String toShortString();

    public static class Builder<E> implements Iterable<E> {
        private InternalBuilder<E> internalBuilder;
        private final ImmutableSet<E> initialImmutableSet;

        public Builder() {
            this.initialImmutableSet = null;

            internalBuilder = new HashArrayBackedSet.Builder<E>(16);
        }

        public Builder(int expectedNumberOfElements) {
            this.initialImmutableSet = null;

            if (expectedNumberOfElements <= 16) {
                internalBuilder = new HashArrayBackedSet.Builder<E>(16);
            } else if (expectedNumberOfElements <= 64) {
                internalBuilder = new HashArrayBackedSet.Builder<E>(64);
            } else if (expectedNumberOfElements <= 256) {
                internalBuilder = new HashArrayBackedSet.Builder<E>(256);
            } else {
                internalBuilder = new SetBackedSet.Builder<E>(expectedNumberOfElements);
            }
        }

        public Builder(Collection<E> initialContent) {
            if (initialContent instanceof ImmutableSet) {
                this.initialImmutableSet = (ImmutableSet<E>) initialContent;
            } else {
                this.initialImmutableSet = null;

                initFrom(initialContent);
            }
        }

        public Builder(E[] initialContent) {
            this.initialImmutableSet = null;

            if (initialContent.length <= 16) {
                internalBuilder = new HashArrayBackedSet.Builder<E>(16);

                for (int i = 0; i < initialContent.length; i++) {
                    internalBuilder = internalBuilder.with(initialContent[i]);
                }
            } else if (initialContent.length <= 64) {
                internalBuilder = new HashArrayBackedSet.Builder<E>(64);

                for (int i = 0; i < initialContent.length; i++) {
                    internalBuilder = internalBuilder.with(initialContent[i]);
                }
            } else if (initialContent.length <= 256) {
                internalBuilder = new HashArrayBackedSet.Builder<E>(256);

                for (int i = 0; i < initialContent.length; i++) {
                    internalBuilder = internalBuilder.with(initialContent[i]);
                }
            } else {
                internalBuilder = new SetBackedSet.Builder<E>(Arrays.asList(initialContent));
            }
        }

        public Builder<E> with(E e) {
            if (internalBuilder == null) {
                initFrom(initialImmutableSet);
            }

            internalBuilder = internalBuilder.with(e);
            return this;
        }

        public Builder<E> with(Collection<E> collection) {
            if (internalBuilder == null) {
                initFrom(initialImmutableSet);
            }

            internalBuilder = internalBuilder.with(collection);
            return this;
        }

        public boolean add(E e) {
            if (internalBuilder == null) {
                initFrom(initialImmutableSet);
            }

            int size = internalBuilder.size();
            internalBuilder = internalBuilder.with(e);
            return size != internalBuilder.size();
        }

        public boolean addAll(Collection<E> collection) {
            if (internalBuilder == null) {
                initFrom(initialImmutableSet);
            }

            int size = internalBuilder.size();

            for (E e : collection) {
                internalBuilder = internalBuilder.with(e);
            }

            return size != internalBuilder.size();
        }

        public boolean addAll(@SuppressWarnings("unchecked") E... array) {
            if (internalBuilder == null) {
                initFrom(initialImmutableSet);
            }

            int size = internalBuilder.size();

            for (int i = 0; i < array.length; i++) {
                internalBuilder = internalBuilder.with(array[i]);
            }

            return size != internalBuilder.size();
        }

        public boolean remove(E e) {
            if (internalBuilder == null) {
                initFrom(initialImmutableSet);
            }

            return internalBuilder.remove(e);
        }

        public void clear() {
            if (internalBuilder == null) {
                internalBuilder = new HashArrayBackedSet.Builder<E>(16);
            } else {
                internalBuilder.clear();
            }
        }

        public boolean contains(E e) {
            if (internalBuilder == null) {
                return initialImmutableSet.contains(e);
            } else {
                return internalBuilder.contains(e);
            }
        }

        public boolean containsAny(Set<E> set) {
            if (internalBuilder == null) {
                return initialImmutableSet.containsAny(set);
            } else {
                return internalBuilder.containsAny(set);
            }
        }

        public boolean containsAll(Set<E> set) {
            if (internalBuilder == null) {
                return initialImmutableSet.containsAll(set);
            } else {
                return internalBuilder.containsAll(set);
            }
        }

        public ImmutableSet<E> build() {
            if (internalBuilder == null) {
                return initialImmutableSet;
            } else {
                return internalBuilder.build();
            }
        }

        public Iterator<E> iterator() {
            if (internalBuilder == null) {
                initFrom(initialImmutableSet);
            }

            return internalBuilder.iterator();
        }

        public int size() {
            if (internalBuilder == null) {
                return initialImmutableSet.size();
            } else {
                return internalBuilder.size();
            }
        }

        public E any() {
            if (internalBuilder == null) {
                return initialImmutableSet.any();
            } else {
                return internalBuilder.any();
            }
        }

        @Override
        public String toString() {
            if (internalBuilder == null) {
                return initialImmutableSet.toString();
            } else {
                return internalBuilder.toString();
            }
        }

        public String toDebugString() {
            return internalBuilder != null ? (internalBuilder.getClass() + " " + internalBuilder.toDebugString()) : "initial state";
        }

        private void initFrom(Collection<E> initialContent) {
            if (initialContent instanceof HashArrayBackedSet) {
                internalBuilder = new HashArrayBackedSet.Builder<E>((HashArrayBackedSet<E>) initialContent);
            } else {
                if (initialContent.size() <= 16) {
                    internalBuilder = new HashArrayBackedSet.Builder<E>(16);

                    for (E e : initialContent) {
                        internalBuilder = internalBuilder.with(e);
                    }
                } else if (initialContent.size() <= 64) {
                    internalBuilder = new HashArrayBackedSet.Builder<E>(64);

                    for (E e : initialContent) {
                        internalBuilder = internalBuilder.with(e);
                    }
                } else if (initialContent.size() <= 256) {
                    internalBuilder = new HashArrayBackedSet.Builder<E>(256);

                    for (E e : initialContent) {
                        internalBuilder = internalBuilder.with(e);
                    }
                } else {
                    internalBuilder = new SetBackedSet.Builder<E>(initialContent);
                }
            }
        }
    }

    static abstract class InternalBuilder<E> implements Iterable<E> {
        abstract InternalBuilder<E> with(E e);

        abstract InternalBuilder<E> with(Collection<E> e);

        abstract boolean remove(E e);

        abstract void clear();

        abstract boolean contains(E e);

        abstract boolean containsAny(Set<E> set);

        abstract boolean containsAll(Set<E> set);

        abstract ImmutableSet<E> build();

        abstract int size();

        public abstract Iterator<E> iterator();

        abstract E any();

        abstract String toDebugString();

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder("[");
            boolean first = true;

            for (E e : this) {
                if (first) {
                    first = false;
                } else {
                    result.append(", ");
                }

                result.append(e);
            }

            result.append("]");
            result.append(" ").append(size()).append(getClass());

            return result.toString();
        }
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

        @Override
        public <O> ImmutableSet<O> map(Function<E, O> mappingFunction) {
            ImmutableSet.Builder<O> builder = new ImmutableSet.Builder<>(size());

            for (E e : this) {
                O o = mappingFunction.apply(e);

                if (o != null) {
                    builder.with(o);
                }
            }

            return builder.build();
        }

        @Override
        public <O> ImmutableSet<O> mapFlat(Function<E, Collection<O>> mappingFunction) {
            ImmutableSet.Builder<O> builder = new ImmutableSet.Builder<>(size());

            for (E e : this) {
                Collection<O> o = mappingFunction.apply(e);

                if (o != null) {
                    builder.with(o);
                }
            }

            return builder.build();
        }

        @Override
        public boolean containsAny(Collection<E> collection) {
            for (E e : collection) {
                if (contains(e)) {
                    return true;
                }
            }

            return false;
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
        public E any() {
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

        @Override
        public <O> ImmutableSet<O> map(Function<E, O> mappingFunction) {
            O o = mappingFunction.apply(e1);

            if (o != null) {
                return new OneElementSet<O>(o);
            } else {
                return empty();
            }
        }

        @Override
        public <O> ImmutableSet<O> mapFlat(Function<E, Collection<O>> mappingFunction) {
            return ImmutableSet.of(mappingFunction.apply(e1));
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
        public E any() {
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

        @Override
        public <O> ImmutableSet<O> map(Function<E, O> mappingFunction) {
            O o1 = mappingFunction.apply(e1);
            O o2 = mappingFunction.apply(e2);

            return ofNonNull(o1, o2);
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

            for (int i = 0; i < this.elements.length; i++) {
                if (this.elements[i] == null) {
                    throw new IllegalArgumentException("ImmutableSet does not support null elements");
                }
            }
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
        public E any() {
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
                return new ArrayBackedSet<E>(newElements2);
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
                return new ArrayBackedSet<E>(newElements2);
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

    static class HashArrayBackedSet<E> extends AbstractImmutableSet<E> {

        private static final int COLLISION_HEAD_ROOM = 4;
        private static final int NO_SPACE = Integer.MAX_VALUE;

        final int tableSize;
        private int size = 0;

        private final E[] table1;
        private final E[] table2;
        private E[] flat;

        HashArrayBackedSet(int tableSize, int size, E[] table1, E[] table2) {
            this.tableSize = tableSize;
            this.size = size;
            this.table1 = table1;
            this.table2 = table2;
        }

        HashArrayBackedSet(int tableSize, int size, E[] table1, E[] table2, E[] flat) {
            this.tableSize = tableSize;
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
            return size == 0;
        }

        @Override
        public boolean contains(Object o) {
            return contains(o, hashPosition(o));
        }

        boolean contains(Object o, int pos) {
            if (o.equals(this.table1[pos])) {
                return true;
            } else if (this.table2 != null && checkTable2(o, pos) < 0) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Iterator<E> iterator() {
            if (isEmpty()) {
                return Collections.emptyIterator();
            }

            return new Iterator<E>() {

                private E[] table = table1;
                private int i = findIndexOfNextNonNull(table1, 0);

                @Override
                public boolean hasNext() {
                    return table != null;
                }

                @Override
                public E next() {
                    if (table == null || i == -1) {
                        throw new NoSuchElementException();
                    }

                    E element = table[i];

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

        @Override
        public E any() {
            return getFlatArray()[0];
        }

        @Override
        public E only() {
            if (size() != 1) {
                throw new IllegalStateException();
            }

            return any();
        }

        @Override
        public ImmutableSet<E> matching(Predicate<E> predicate) {
            int table1count = 0;
            int table2count = 0;

            E[] newTable1 = createTable1();
            E[] newTable2 = table2 != null ? createTable2() : null;
            E[] newFlat = createEArray(size);

            for (int i = 0; i < tableSize; i++) {
                E v = this.table1[i];

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
                for (int i = 0; i < this.table2.length; i++) {
                    E v = this.table2[i];

                    if (v != null) {
                        if (predicate.test(v)) {
                            int pos = i == 0 ? 0 : hashPosition(v);

                            if (newTable1[pos] == null) {
                                newTable1[pos] = v;
                                table1count++;
                            } else {
                                for (int k = pos;; k++) {
                                    if (newTable2[k] == null) {
                                        newTable2[k] = v;
                                        table2count++;
                                        break;
                                    }
                                }
                            }

                            newFlat[count] = v;
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
                    return new HashArrayBackedSet<E>(tableSize, count, newTable1, null, newFlat);
                } else if (table1count == 0) {
                    return new HashArrayBackedSet<E>(tableSize, count, newTable2, null, newFlat);
                } else {
                    return new HashArrayBackedSet<E>(tableSize, count, newTable1, newTable2, newFlat);
                }
            } else {
                return this;
            }
        }

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

            if (other instanceof HashArrayBackedSet && ((HashArrayBackedSet<E>) other).tableSize == tableSize) {
                return intersection((HashArrayBackedSet<E>) other);
            }

            return matching((e) -> other.contains(e));
        }

        private ImmutableSet<E> intersection(HashArrayBackedSet<E> other) {
            int table1count = 0;
            int table2count = 0;

            E[] newTable1 = createTable1();
            E[] newTable2 = table2 != null ? createTable2() : null;

            for (int i = 0; i < tableSize; i++) {
                E v = this.table1[i];

                if (v != null) {
                    if (other.contains(v, i)) {
                        newTable1[i] = v;
                        table1count++;
                    }
                }
            }

            if (this.table2 != null) {
                for (int i = 0; i < this.table2.length; i++) {
                    E v = this.table2[i];

                    if (v != null) {
                        int pos = i == 0 ? 0 : hashPosition(v);

                        if (other.contains(v, pos)) {
                            if (newTable1[pos] == null) {
                                newTable1[pos] = v;
                                table1count++;
                            } else {
                                for (int k = pos;; k++) {
                                    if (newTable2[k] == null) {
                                        newTable2[k] = v;
                                        table2count++;
                                        break;
                                    }
                                }
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
                    return new HashArrayBackedSet<E>(tableSize, count, newTable1, null);
                } else if (table1count == 0) {
                    return new HashArrayBackedSet<E>(tableSize, count, newTable2, null);
                } else {
                    return new HashArrayBackedSet<E>(tableSize, count, newTable1, newTable2);
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
                        int check = checkTable2(other, pos);

                        if (check < 0) {
                            // already contained
                            return this;
                        } else if (check == NO_SPACE) {
                            return new WithSet<>(this, other);
                        } else {
                            E[] newTable2 = this.table2.clone();
                            newTable2[check] = other;
                            return new HashArrayBackedSet<>(tableSize, size + 1, this.table1, newTable2);
                        }
                    } else {
                        E[] newTable2 = this.table2.clone();
                        newTable2[pos] = other;
                        return new HashArrayBackedSet<>(tableSize, size + 1, this.table1, newTable2);
                    }
                } else {
                    E[] newTable2 = createTable2();
                    newTable2[pos] = other;
                    return new HashArrayBackedSet<>(tableSize, size + 1, this.table1, newTable2);
                }
            } else {
                E[] newTable1 = this.table1.clone();
                newTable1[pos] = other;
                return new HashArrayBackedSet<>(tableSize, size + 1, newTable1, this.table2);
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
                InternalBuilder<E> builder = new HashArrayBackedSet.Builder<E>(this);
                builder = builder.with(other);
                return builder.build();
            }
        }

        @Override
        public ImmutableSet<E> with(@SuppressWarnings("unchecked") E... other) {
            int otherSize = other.length;

            if (otherSize == 0) {
                return this;
            } else if (otherSize == 1) {
                return this.with(other[0]);
            } else {
                InternalBuilder<E> builder = new HashArrayBackedSet.Builder<E>(this);

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

                E[] newTable1 = this.table1.clone();
                newTable1[pos] = null;

                if (this.table2 == null || this.table2[pos] == null) {
                    return new HashArrayBackedSet<>(tableSize, size - 1, newTable1, this.table2);
                } else {
                    for (int i = pos; i < this.table2.length && this.table2[i] != null; i++) {
                        int otherPos = hashPosition(this.table2[i]);
                        if (otherPos == pos) {
                            newTable1[pos] = this.table2[i];
                            E[] newTable2 = this.table2.clone();
                            newTable2[i] = null;
                            repositionCollisions(tableSize, newTable2, i);
                            return new HashArrayBackedSet<>(tableSize, size - 1, newTable1, newTable2);
                        }
                    }

                    return new HashArrayBackedSet<>(tableSize, size - 1, newTable1, this.table2);
                }

            } else if (this.table2 != null && this.table2[pos] != null) {
                int check = checkTable2(other, pos);

                if (check < 0) {
                    // Contained

                    if (size == 1) {
                        return empty();
                    }

                    int actualPos = -check - 1;

                    E[] newTable2 = this.table2.clone();
                    newTable2[actualPos] = null;
                    repositionCollisions(tableSize, newTable2, actualPos);
                    return new HashArrayBackedSet<>(tableSize, size - 1, this.table1, newTable2);
                } else {
                    // Not contained
                    return this;
                }

            } else {
                return this;
            }
        }

        private static <E> void repositionCollisions(int tableSize, E[] table2, int start) {
            assert table2[start] == null;

            int firstGapAt = -1;
            int lastGapAt = -1;

            for (int i = start + 1; i < table2.length; i++) {
                if (table2[i] == null) {
                    // done
                    return;
                }

                int pos = hashPosition(tableSize, table2[i]);

                if (firstGapAt == -1) {
                    if (pos != i) {
                        table2[i - 1] = table2[i];
                        table2[i] = null;
                    } else {
                        firstGapAt = i - 1;
                        lastGapAt = i - 1;
                    }
                } else {
                    if (pos == i) {
                        if (table2[i - 1] == null) {
                            lastGapAt = i - 1;
                        }
                    } else if (pos == lastGapAt) {
                        assert table2[lastGapAt] == null;

                        table2[lastGapAt] = table2[i];
                        table2[i] = null;

                        lastGapAt = -1;

                    } else {
                        for (int k = i - 1; k >= firstGapAt; k--) {
                            if (k < pos) {
                                break;
                            }

                            if (table2[k] == null) {
                                table2[k] = table2[i];
                                table2[i] = null;
                                lastGapAt = -1;
                                break;
                            }
                        }

                    }
                }
            }
        }

        E[] getFlatArray() {
            if (flat != null) {
                return flat;
            }

            E[] flat = createEArray(size);
            int k = 0;

            for (int i = 0; i < table1.length; i++) {
                E v = table1[i];

                if (v != null) {
                    flat[k] = v;
                    k++;
                }
            }

            if (table2 != null) {
                for (int i = 0; i < table2.length; i++) {
                    E v = table2[i];

                    if (v != null) {
                        flat[k] = v;
                        k++;
                    }
                }
            }

            this.flat = flat;

            return flat;
        }

        @SuppressWarnings("unchecked")
        private E[] createTable1() {
            return (E[]) new Object[tableSize];
        }

        @SuppressWarnings("unchecked")
        private E[] createTable2() {
            return (E[]) new Object[tableSize + COLLISION_HEAD_ROOM];
        }

        @SuppressWarnings("unchecked")
        private static <E> E[] createTable1(int tableSize) {
            return (E[]) new Object[tableSize];
        }

        @SuppressWarnings("unchecked")
        private static <E> E[] createTable2(int tableSize) {
            return (E[]) new Object[tableSize + COLLISION_HEAD_ROOM];
        }

        @SuppressWarnings("unchecked")
        private E[] createEArray(int size) {
            return (E[]) new Object[size];

        }

        static <E> E findFirstNonNull(E[] array) {
            for (int i = 0; i < array.length; i++) {
                if (array[i] != null) {
                    return array[i];
                }
            }

            return null;
        }

        static <E> E findFirstNonNull(E[] array1, E[] array2) {
            E result = findFirstNonNull(array1);

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

        int hashPosition(Object e) {
            return hashPosition(tableSize, e);
        }

        static int hashPosition(int tableSize, Object e) {
            if (e == null) {
                throw new IllegalArgumentException("ImmutableSet does not support null values");
            }

            int hash = e.hashCode();

            switch (tableSize) {
            case 16:
                return (hash & 0xf) ^ (hash >> 4 & 0xf) ^ (hash >> 8 & 0xf) ^ (hash >> 12 & 0xf) ^ (hash >> 16 & 0xf) ^ (hash >> 20 & 0xf)
                        ^ (hash >> 24 & 0xf) ^ (hash >> 28 & 0xf);
            case 64:
                return (hash & 0x3f) ^ (hash >> 6 & 0x3f) ^ (hash >> 12 & 0x3f) ^ (hash >> 18 & 0x3f) ^ (hash >> 24 & 0xf) ^ (hash >> 28 & 0xf);
            case 256:
                return (hash & 0xff) ^ (hash >> 8 & 0xff) ^ (hash >> 16 & 0xff) ^ (hash >> 24 & 0xff);
            default:
                throw new RuntimeException("Invalid tableSize " + tableSize);
            }

        }

        /** 
         * If e is contained in table2: returns the position as negative value calculated by -1 - position.
         * If e is not contained in table2: returns a possible free slot in table2 as positive value. If no slot is free, NO_SPACE is returned.
         */
        int checkTable2(Object e, int hashPosition) {
            return checkTable2(table2, e, hashPosition);
        }

        /** 
         * If e is contained in table2: returns the position as negative value calculated by -1 - position.
         * If e is not contained in table2: returns a possible free slot in table2 as positive value. If no slot is free, NO_SPACE is returned.
         */
        static <E> int checkTable2(E[] table2, Object e, int hashPosition) {
            if (table2[hashPosition] == null) {
                return hashPosition;
            } else if (table2[hashPosition].equals(e)) {
                return -1 - hashPosition;
            }

            int max = hashPosition + COLLISION_HEAD_ROOM;

            for (int i = hashPosition + 1; i <= max; i++) {
                if (table2[i] == null) {
                    return i;
                } else if (table2[i].equals(e)) {
                    return -1 - i;
                }
            }

            return NO_SPACE;
        }

        static class Builder<E> extends InternalBuilder<E> {
            private final static Object T = new Object();
            private E[] table1;
            private E[] table2;
            private int size = 0;
            private final int tableSize;
            private boolean containsTombstones = false;
            @SuppressWarnings("unchecked")
            private final E tombstone = (E) T;

            public Builder(int tableSize) {
                this.tableSize = tableSize;
            }

            public Builder(HashArrayBackedSet<E> initialContent) {
                this.table1 = initialContent.table1.clone();
                this.table2 = initialContent.table2 != null ? initialContent.table2.clone() : null;
                this.size = initialContent.size;
                this.tableSize = initialContent.tableSize;
            }

            public InternalBuilder<E> with(E e) {
                if (e == null) {
                    throw new IllegalArgumentException("Null elements are not supported");
                }

                if (table1 == null) {
                    table1 = createTable1(tableSize);
                    table1[hashPosition(e)] = e;
                    size++;
                    return this;
                } else {
                    int position = hashPosition(e);

                    if (table1[position] == null) {
                        table1[position] = e;
                        size++;
                        return this;
                    } else if (table1[position].equals(e)) {
                        // done
                        return this;
                    } else {
                        // collision

                        if (table2 == null) {
                            table2 = createTable2(tableSize);
                            table2[position] = e;
                            size++;
                            return this;
                        } else if (table2[position] == null) {
                            table2[position] = e;
                            size++;
                            return this;
                        } else {
                            int check = checkTable2(e, position);

                            if (check < 0) {
                                // done     
                                return this;
                            } else if (check == NO_SPACE) {
                                // collision
                                if (tableSize < 64) {
                                    return new HashArrayBackedSet.Builder<E>(64).with(build()).with(e);
                                } else if (tableSize < 256) {
                                    return new HashArrayBackedSet.Builder<E>(256).with(build()).with(e);
                                } else {
                                    return new SetBackedSet.Builder<>(build()).with(e);
                                }

                            } else {
                                table2[check] = e;
                                size++;
                                return this;
                            }
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

            public ImmutableSet<E> build() {
                if (size == 0) {
                    return ImmutableSet.empty();
                } else if (size == 1) {
                    E e = (E) findFirstNonNull(this.table1);
                    return new OneElementSet<>(e);
                } else if (size == 2) {
                    int i1 = findIndexOfNextNonNull(this.table1, 0);
                    E e1 = this.table1[i1];
                    int i2 = findIndexOfNextNonNull(this.table1, i1 + 1);
                    E e2;

                    if (i2 != -1) {
                        e2 = this.table1[i2];
                    } else {
                        i2 = findIndexOfNextNonNullNonTombstone(this.table2, 0);
                        e2 = this.table2[i2];
                    }

                    return new TwoElementSet<>(e1, e2);
                } else {
                    clearTombstones();
                    return new HashArrayBackedSet<>(tableSize, size, table1, table2);
                }
            }

            private void clearTombstones() {
                if (!containsTombstones) {
                    return;
                }

                int max = tableSize + COLLISION_HEAD_ROOM - 1;
                int table2count = 0;

                for (int i = 0; i <= max; i++) {
                    if (table2[i] == tombstone) {
                        table2[i] = null;

                        int kMax = i >= tableSize ? max : i + COLLISION_HEAD_ROOM - 1;

                        for (int k = kMax; k > i; k--) {
                            if (table2[k] != null && table2[k] != tombstone && hashPosition(table2[k]) <= i) {
                                table2[i] = table2[k];
                                table2[k] = tombstone;
                                break;
                            }
                        }

                    } else if (table2[i] != null) {
                        table2count++;
                    }
                }

                if (table2count == 0) {
                    table2 = null;
                }
            }

            @Override
            int size() {
                return size;
            }

            @Override
            boolean remove(E e) {
                int position = hashPosition(e);

                if (table1[position] != null && table1[position].equals(e)) {
                    table1[position] = null;
                    size--;

                    if (table2 != null && table2[position] != null) {
                        table1[position] = pullElementWithHashPositionFromTable2(position);
                    }

                    return true;
                } else if (table2 != null && table2[position] != null) {

                    int check = checkTable2(e, position);

                    if (check < 0) {
                        // Contained
                        int actualPos = -check - 1;
                        table2[actualPos] = tombstone;
                        containsTombstones = true;
                        size--;
                        // repositionCollisions(tableSize, table2, actualPos);

                        return true;
                    }
                }

                // Not contained
                return false;
            }

            @Override
            void clear() {
                size = 0;

                if (table1 != null) {
                    Arrays.fill(table1, null);
                }

                if (table2 != null) {
                    Arrays.fill(table2, null);
                }
            }

            @Override
            boolean contains(E e) {
                int position = hashPosition(e);

                if (table1[position] != null && table1[position].equals(e)) {
                    return true;
                } else if (table2 != null && table2[position] != null) {
                    int check = checkTable2(e, position);

                    return check < 0;
                }

                return false;
            }

            @Override
            boolean containsAny(Set<E> set) {
                if (set instanceof HashArrayBackedSet && ((HashArrayBackedSet<E>) set).tableSize == tableSize) {
                    return containsAny((HashArrayBackedSet<E>) set);
                } else {
                    for (E e : set) {
                        if (contains(e)) {
                            return true;
                        }
                    }
                }

                return false;
            }

            private boolean containsAny(HashArrayBackedSet<E> set) {
                for (int i = 0; i < tableSize; i++) {
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
                if (set instanceof HashArrayBackedSet && ((HashArrayBackedSet<E>) set).tableSize == tableSize) {
                    return containsAll((HashArrayBackedSet<E>) set);
                } else {
                    for (E e : set) {
                        if (!contains(e)) {
                            return false;
                        }
                    }

                    return true;
                }
            }

            private boolean containsAll(HashArrayBackedSet<E> set) {
                for (int i = 0; i < tableSize; i++) {
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
                if (table1 != null && table1[hashPosition] != null && table1[hashPosition].equals(e)) {
                    return true;
                } else if (table2 != null && table2[hashPosition] != null) {
                    int check = checkTable2(e, hashPosition);

                    return check < 0;
                } else {
                    return false;
                }
            }

            @Override
            public Iterator<E> iterator() {
                if (size == 0) {
                    return Collections.emptyIterator();
                }

                return new Iterator<E>() {

                    int state = 0;
                    int nextState = 1;
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

                        if (prevState == 1) {
                            if (table1 != null && prev.equals(table1[this.prevPos])) {
                                table1[this.prevPos] = null;
                                size--;

                                if (table2 != null && table2[this.prevPos] != null) {
                                    table1[this.prevPos] = pullElementWithHashPositionFromTable2(this.prevPos);

                                    if (table1[this.prevPos] != null) {
                                        nextState = 1;
                                        nextNextPos = this.prevPos;
                                    }
                                }

                            }
                        } else if (prevState == 2) {
                            if (table2 != null && prev.equals(table2[this.prevPos])) {
                                table2[this.prevPos] = tombstone;
                                containsTombstones = true;
                                size--;
                            }
                        }

                    }

                    private void initNext() {

                        if (nextState == 1) {
                            state = 1;
                            if (table1 != null) {
                                int nextIndex = findIndexOfNextNonNull(table1, nextNextPos);

                                if (nextIndex != -1) {
                                    this.nextPos = nextIndex;
                                    this.nextNextPos = nextIndex + 1;
                                    this.next = (E) table1[nextIndex];
                                    return;
                                } else {
                                    this.nextNextPos = 0;
                                    this.nextState = 2;
                                }
                            } else {
                                this.nextState = 2;
                            }
                        }

                        if (nextState == 2) {
                            state = 2;
                            if (table2 != null) {
                                int nextIndex = findIndexOfNextNonNullNonTombstone(table2, nextNextPos);

                                if (nextIndex != -1) {
                                    this.nextPos = nextIndex;
                                    this.nextNextPos = nextIndex + 1;
                                    this.next = (E) table2[nextIndex];
                                    return;
                                } else {
                                    this.nextNextPos = 0;
                                    this.nextState = 2;
                                }
                            } else {
                                this.nextState = 3;
                            }
                        }

                        this.next = null;
                    }

                };
            }

            @Override
            E any() {
                return (E) findFirstNonNull(table1, table2);
            }

            private int hashPosition(Object e) {
                return HashArrayBackedSet.hashPosition(tableSize, e);
            }

            @Override
            String toDebugString() {
                return "size: " + size + "; tail1: " + (table1 != null ? Arrays.asList(table1).toString() : "null")
                        + ("; tail2: " + (table2 != null ? Arrays.asList(table2).toString() : "null"));
            }

            /** 
             * If e is contained in table2: returns the position as negative value calculated by -1 - position.
             * If e is not contained in table2: returns a possible free slot in table2 as positive value. If no slot is free, NO_SPACE is returned.
             */
            int checkTable2(Object e, int hashPosition) {
                int insertionPositionCandidate = NO_SPACE;

                if (table2[hashPosition] == null) {
                    return hashPosition;
                } else if (table2[hashPosition] == tombstone) {
                    insertionPositionCandidate = hashPosition;
                } else if (table2[hashPosition].equals(e)) {
                    return -1 - hashPosition;
                }

                int max = hashPosition + COLLISION_HEAD_ROOM;

                for (int i = hashPosition + 1; i <= max; i++) {
                    if (table2[i] == null) {
                        if (insertionPositionCandidate != NO_SPACE) {
                            return insertionPositionCandidate;
                        } else {
                            return i;
                        }
                    } else if (table2[i] == tombstone) {
                        if (insertionPositionCandidate == NO_SPACE) {
                            insertionPositionCandidate = i;
                        }
                    } else if (table2[i].equals(e)) {
                        return -1 - i;
                    }
                }

                return insertionPositionCandidate;
            }

            E pullElementWithHashPositionFromTable2(int hashPosition) {
                int max = hashPosition + COLLISION_HEAD_ROOM;

                for (int i = hashPosition; i <= max; i++) {
                    if (table2[i] == null) {
                        return null;
                    } else if (table2[i] != tombstone && hashPosition(table2[i]) == hashPosition) {
                        E result = table2[i];
                        table2[i] = tombstone;
                        containsTombstones = true;
                        return result;
                    }
                }

                return null;
            }

            int findIndexOfNextNonNullNonTombstone(Object[] array, int start) {
                for (int i = start; i < array.length; i++) {
                    if (array[i] != null && array[i] != tombstone) {
                        return i;
                    }
                }

                return -1;
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
            public Iterator<E> iterator() {
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

            @Override
            public String toString() {
                return delegate.toString();
            }

            @Override
            String toDebugString() {
                return delegate.toString();
            }

        }

    }

    static class EmptySet<E> extends AbstractImmutableSet<E> {

        static EmptySet<?> INSTANCE = new EmptySet<Object>();

        EmptySet() {
        }

        @Override
        public E any() {
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
        public E any() {
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
