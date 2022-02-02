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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public interface ImmutableList<E> extends List<E> {

    public static <E> ImmutableList<E> empty() {
        @SuppressWarnings("unchecked")
        ImmutableList<E> result = (ImmutableList<E>) EmptyList.INSTANCE;
        return result;
    }

    public static <E> ImmutableList<E> of(E e) {
        return new OneElementList<E>(e);
    }

    public static <E> ImmutableList<E> of(E e1, E e2) {

        return new TwoElementList<E>(e1, e2);
    }

    @SafeVarargs
    public static <E> ImmutableList<E> of(E e, E... more) {
        if (e == null) {
            return ofArray(more);
        } else if (more == null || more.length == 0) {
            return new OneElementList<E>(e);
        } else {
            return new Builder<>(Arrays.asList(more)).with(e).build();
        }
    }

    @SafeVarargs
    public static <E> ImmutableList<E> ofArray(E... more) {
        if (more == null || more.length == 0) {
            return empty();
        } else if (more.length == 1) {
            return new OneElementList<>(more[0]);
        } else if (more.length == 2) {
            return new TwoElementList<>(more[0], more[1]);
        } else {
            return new ArrayBackedList<>(more);
        }
    }

    public static <E> ImmutableList<E> of(Collection<? extends E> collection) {
        if (collection == null || collection.size() == 0) {
            return empty();
        } else if (collection instanceof ImmutableList) {
            @SuppressWarnings("unchecked")
            ImmutableList<E> result = (ImmutableList<E>) collection;
            return result;
        } else if (collection.size() == 1) {
            return new OneElementList<>(collection.iterator().next());
        } else if (collection.size() == 2) {
            Iterator<? extends E> iter = collection.iterator();
            E e1 = iter.next();
            E e2 = iter.next();

            return new TwoElementList<>(e1, e2);
        } else {
            return new ArrayBackedList<>(collection.toArray());
        }
    }

    public static <E> ImmutableList<E> concat(Collection<? extends E> c1, Collection<? extends E> c2) {
        if (c1 == null || c1.size() == 0) {
            return of(c2);
        } else if (c2 == null || c2.size() == 0) {
            return of(c1);
        } else {
            Object[] array = new Object[c1.size() + c2.size()];

            int i = 0;

            for (E e : c1) {
                array[i] = e;
                i++;
            }

            for (E e : c2) {
                array[i] = e;
                i++;
            }

            return new ArrayBackedList<>(array);
        }
    }

    public static <E> ImmutableList<E> concat(Collection<? extends E> c1, Collection<? extends E> c2, Collection<? extends E> c3) {
        if (c1 == null || c1.size() == 0) {
            return concat(c2, c3);
        } else if (c2 == null || c2.size() == 0) {
            return concat(c1, c3);
        } else if (c3 == null || c3.size() == 0) {
            return concat(c1, c2);
        } else {
            Object[] array = new Object[c1.size() + c2.size() + c3.size()];

            int i = 0;

            for (E e : c1) {
                array[i] = e;
                i++;
            }

            for (E e : c2) {
                array[i] = e;
                i++;
            }

            for (E e : c3) {
                array[i] = e;
                i++;
            }

            return new ArrayBackedList<>(array);
        }
    }

    public static <C, E> ImmutableList<E> map(Collection<C> collection, Function<C, E> mappingFunction) {
        ImmutableList.Builder<E> builder = new ImmutableList.Builder<>(collection.size());

        for (C c : collection) {
            E value = mappingFunction.apply(c);

            if (value != null) {
                builder.with(value);
            }
        }

        return builder.build();
    }

    ImmutableList<E> with(E other);

    ImmutableList<E> with(Collection<E> other);

    ImmutableList<E> with(Optional<E> other);

    ImmutableList<E> with(@SuppressWarnings("unchecked") E... other);

    ImmutableList<E> matching(Predicate<E> predicate);

    ImmutableList<E> without(Collection<E> other);

    <O> ImmutableList<O> map(Function<E, O> mappingFunction);

    E only();

    String toShortString();

    public static class Builder<E> {
        private ArrayList<E> list;

        public Builder() {
            this(10);
        }

        public Builder(int expectedNumberOfElements) {
            this.list = new ArrayList<>(expectedNumberOfElements);
        }

        public Builder(Collection<E> initialContent) {
            this.list = new ArrayList<>(initialContent);
        }

        public Builder<E> with(E e) {
            list.add(e);
            return this;
        }

        public Builder<E> with(Optional<E> optional) {
            if (optional.isPresent()) {
                list.add(optional.get());
            }
            return this;
        }

        public Builder<E> with(Collection<E> collection) {
            list.addAll(collection);
            return this;
        }

        public ImmutableList<E> build() {
            return of(list);
        }

        public ImmutableList<E> build(Comparator<E> sortedBy) {
            list.sort(sortedBy);
            return of(list);
        }

    }

    static abstract class AbstractImmutableList<E> extends AbstractImmutableCollection<E> implements ImmutableList<E> {

        private int hashCode = -1;

        public ImmutableList<E> with(E other) {
            int size = size();

            if (size == 0) {
                return new OneElementList<E>(other);
            } else if (size == 1) {
                return new TwoElementList<E>(only(), other);
            } else {
                return new Builder<>(this).with(other).build();
            }
        }

        public ImmutableList<E> with(Collection<E> other) {
            int size = size();
            int otherSize = other.size();

            if (size == 0) {
                return ImmutableList.of(other);
            } else if (otherSize == 0) {
                return this;
            } else if (other.size() == 1) {
                return with(other.iterator().next());
            } else {
                return new Builder<E>(this).with(other).build();
            }
        }

        @Override
        public ImmutableList<E> with(Optional<E> other) {
            if (other.isPresent()) {
                return with(other.get());
            } else {
                return this;
            }
        }

        @SuppressWarnings("unchecked")
        public ImmutableList<E> with(E... other) {
            if (other == null || other.length == 0) {
                return this;
            }

            int size = size();
            int otherSize = other.length;

            if (size == 0) {
                return ImmutableList.ofArray(other);
            } else if (otherSize == 1) {
                return with(other[0]);
            }

            return new Builder<E>(this).with(Arrays.asList(other)).build();
        }

        @Override
        public ImmutableList<E> without(Collection<E> other) {
            return matching((e) -> !other.contains(e));
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }

            if (o instanceof List) {
                List<?> otherList = (List<?>) o;

                if (otherList.size() != this.size()) {
                    return false;
                }

                Iterator<E> iter1 = this.iterator();
                Iterator<?> iter2 = otherList.iterator();

                while (iter1.hasNext()) {
                    if (!iter1.next().equals(iter2.next())) {
                        return false;
                    }
                }

                return true;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            if (hashCode == -1) {
                int newHashCode = 0;

                for (E e : this) {
                    newHashCode = 31 * newHashCode + e.hashCode();
                }

                this.hashCode = newHashCode;
            }

            return this.hashCode;
        }

        @Override
        public boolean addAll(int index, Collection<? extends E> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public E set(int index, E element) {
            throw new UnsupportedOperationException();

        }

        @Override
        public void add(int index, E element) {
            throw new UnsupportedOperationException();

        }

        @Override
        public E remove(int index) {
            throw new UnsupportedOperationException();

        }

        @Override
        public ListIterator<E> listIterator() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ListIterator<E> listIterator(int index) {
            // TODO Auto-generated method stub
            return null;
        }

    }

    static class OneElementList<E> extends AbstractImmutableList<E> {

        private final E e1;

        OneElementList(E e1) {
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
        public ImmutableList<E> matching(Predicate<E> predicate) {
            if (predicate.test(e1)) {
                return this;
            } else {
                return empty();
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
        public E get(int index) {
            if (index == 0) {
                return e1;
            } else {
                throw new IndexOutOfBoundsException("index out of bounds: " + index);
            }
        }

        @Override
        public int indexOf(Object o) {
            if (e1.equals(o)) {
                return 0;
            } else {
                return -1;
            }
        }

        @Override
        public int lastIndexOf(Object o) {
            return indexOf(o);
        }

        @Override
        public List<E> subList(int fromIndex, int toIndex) {
            if (fromIndex == 0 && toIndex == 0) {
                return empty();
            } else if (fromIndex == 0 && toIndex == 1) {
                return this;
            } else {
                throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + "; toIndex: " + toIndex);
            }
        }

        @Override
        public <O> ImmutableList<O> map(Function<E, O> mappingFunction) {
            O o1 = mappingFunction.apply(e1);

            if (o1 != null) {
                return new OneElementList<O>(o1);
            } else {
                return empty();
            }
        }

        @Override
        public ImmutableList<E> with(Optional<E> other) {
            if (other.isPresent()) {
                return new TwoElementList<>(e1, other.get());
            } else {
                return this;
            }
        }
    }

    static class TwoElementList<E> extends AbstractImmutableList<E> {

        private final E e1;
        private final E e2;

        TwoElementList(E e1, E e2) {
            this.e1 = e1;
            this.e2 = e2;
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
        public ImmutableList<E> matching(Predicate<E> predicate) {
            if (predicate.test(e1)) {
                if (predicate.test(e2)) {
                    return this;
                } else {
                    return new OneElementList<>(e1);
                }
            } else if (predicate.test(e2)) {
                return new OneElementList<>(e2);
            } else {
                return empty();
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
        public E get(int index) {
            if (index == 0) {
                return e1;
            } else if (index == 1) {
                return e2;
            } else {
                throw new IndexOutOfBoundsException("index out of bounds: " + index);
            }
        }

        @Override
        public int indexOf(Object o) {
            if (e1.equals(o)) {
                return 0;
            } else if (e2.equals(o)) {
                return 1;
            } else {
                return -1;
            }
        }

        @Override
        public int lastIndexOf(Object o) {
            if (e2.equals(o)) {
                return 0;
            } else if (e1.equals(o)) {
                return 1;
            } else {
                return -1;
            }
        }

        @Override
        public List<E> subList(int fromIndex, int toIndex) {
            if (fromIndex == 0 && toIndex == 0) {
                return empty();
            } else if (fromIndex == 0 && toIndex == 1) {
                return new OneElementList<>(e1);
            } else if (fromIndex == 1 && toIndex == 2) {
                return new OneElementList<>(e2);
            } else if (fromIndex == 0 && toIndex == 2) {
                return this;
            } else {
                throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + "; toIndex: " + toIndex);
            }
        }

        @Override
        public <O> ImmutableList<O> map(Function<E, O> mappingFunction) {
            O o1 = mappingFunction.apply(e1);
            O o2 = mappingFunction.apply(e2);

            if (o1 != null) {
                if (o2 != null) {
                    return new TwoElementList<O>(o1, o2);
                } else {
                    return new OneElementList<O>(o1);
                }
            } else if (o2 != null) {
                return new OneElementList<O>(o2);
            } else {
                return empty();
            }

        }

    }

    static class ArrayBackedList<E> extends AbstractImmutableList<E> {

        private final Object[] elements;
        private String cachedToShortString;

        ArrayBackedList(Object[] elements) {
            this.elements = elements;
        }

        ArrayBackedList(Set<E> elements) {
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
        public ImmutableList<E> matching(Predicate<E> predicate) {
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
                return new OneElementList<E>((E) newElements[0]);
            } else if (k == 2) {
                return new TwoElementList<E>((E) newElements[0], (E) newElements[1]);
            } else if (k < this.elements.length) {
                Object[] newElements2 = new Object[k];
                System.arraycopy(newElements, 0, newElements2, 0, k);
                return new ArrayBackedList<E>(elements);
            } else {
                return this;
            }
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
        public E get(int index) {
            return (E) this.elements[index];
        }

        @Override
        public int indexOf(Object o) {
            for (int i = 0; i < this.elements.length; i++) {
                if (this.elements[i].equals(o)) {
                    return i;
                }
            }

            return -1;
        }

        @Override
        public int lastIndexOf(Object o) {
            for (int i = elements.length - 1; i >= 0; i--) {
                if (this.elements[i].equals(o)) {
                    return i;
                }
            }

            return -1;
        }

        @Override
        public List<E> subList(int fromIndex, int toIndex) {
            if (fromIndex == toIndex) {
                return empty();
            }

            Object[] newElements = new Object[toIndex - fromIndex];
            System.arraycopy(this.elements, fromIndex, newElements, 0, newElements.length);

            return new ArrayBackedList<>(newElements);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <O> ImmutableList<O> map(Function<E, O> mappingFunction) {
            Object[] newArray = new Object[elements.length];

            int k = 0;

            for (int i = 0; i < elements.length; i++) {
                O newValue = mappingFunction.apply((E) elements[i]);

                if (newValue != null) {
                    newArray[k] = newValue;
                    k++;
                }
            }

            if (k == 0) {
                return empty();
            } else if (k == elements.length) {
                return new ArrayBackedList<O>(newArray);
            } else {
                Object[] subArray = new Object[k];
                System.arraycopy(newArray, 0, subArray, 0, k);
                return new ArrayBackedList<O>(subArray);
            }

        }
    }

    static class EmptyList<E> extends AbstractImmutableList<E> {

        static EmptyList<?> INSTANCE = new EmptyList<Object>();

        EmptyList() {
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
        public ImmutableList<E> matching(Predicate<E> predicate) {
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

        @Override
        public E get(int index) {
            return null;
        }

        @Override
        public int indexOf(Object o) {
            return -1;
        }

        @Override
        public int lastIndexOf(Object o) {
            return -1;
        }

        @Override
        public List<E> subList(int fromIndex, int toIndex) {
            if (fromIndex == 0 && toIndex == 0) {
                return empty();
            } else {
                throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + "; toIndex: " + toIndex);
            }
        }

        @Override
        public <O> ImmutableList<O> map(Function<E, O> mappingFunction) {
            return empty();
        }

    }
}
