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

import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

public interface CheckList<E> {
    public static <E> CheckList<E> create(Set<E> elements) {
        if (elements.size() == 2) {
            Iterator<E> iter = elements.iterator();

            return new TwoElementCheckList<E>(iter.next(), iter.next(), "element",
                    elements instanceof ImmutableSet ? (ImmutableSet<E>) elements : null);
        } else {
            return new BasicCheckList<E>(elements, "element");
        }
    }

    public static <E> CheckList<E> create(Set<E> elements, String elementName) {
        if (elements.size() == 2) {
            Iterator<E> iter = elements.iterator();

            return new TwoElementCheckList<E>(iter.next(), iter.next(), elementName,
                    elements instanceof ImmutableSet ? (ImmutableSet<E>) elements : null);
        } else {
            return new BasicCheckList<E>(elements, elementName);
        }
    }

    @SuppressWarnings("unchecked")
    public static <E> CheckList<E> empty() {
        return (CheckList<E>) EMPTY;
    }

    boolean check(E element);

    void uncheck(E element);

    void uncheckIfPresent(E element);

    boolean checkIf(Predicate<E> checkPredicate);

    void uncheckIf(Predicate<E> checkPredicate);

    void checkAll();

    void uncheckAll();

    boolean isChecked(E element);

    boolean isComplete();

    boolean isBlank();

    int size();

    CheckList<E> getView(Predicate<E> predicate);

    CheckList<E> getView(ImmutableSet<E> elementSubSet);

    ImmutableSet<E> getElements();

    ImmutableSet<E> getCheckedElements();

    ImmutableSet<E> getUncheckedElements();

    static class TwoElementCheckList<E> implements CheckList<E> {

        private final E e1;
        private final E e2;
        private final String elementName;
        private ImmutableSet<E> elements;
        private boolean e1checked;
        private boolean e2checked;

        TwoElementCheckList(E e1, E e2, String elementName) {
            this.e1 = e1;
            this.e2 = e2;
            this.elementName = elementName;
        }

        TwoElementCheckList(E e1, E e2, String elementName, ImmutableSet<E> elements) {
            this.e1 = e1;
            this.e2 = e2;
            this.elementName = elementName;
            this.elements = elements;
        }

        @Override
        public boolean check(E element) {
            if (element.equals(e1)) {
                e1checked = true;
            } else if (element.equals(e2)) {
                e2checked = true;
            } else {
                throw new IllegalArgumentException("Invalid " + elementName + ": " + element);
            }

            return e1checked && e2checked;
        }

        @Override
        public void uncheck(E element) {
            if (element.equals(e1)) {
                e1checked = false;
            } else if (element.equals(e2)) {
                e2checked = false;
            } else {
                throw new IllegalArgumentException("Invalid " + elementName + ": " + element);
            }
        }

        @Override
        public void uncheckIfPresent(E element) {
            if (element.equals(e1)) {
                e1checked = false;
            } else if (element.equals(e2)) {
                e2checked = false;
            }
        }

        @Override
        public boolean checkIf(Predicate<E> checkPredicate) {
            if (!e1checked && checkPredicate.test(e1)) {
                e1checked = true;
            }

            if (!e2checked && checkPredicate.test(e2)) {
                e2checked = true;
            }

            return e1checked && e2checked;

        }

        @Override
        public void uncheckIf(Predicate<E> checkPredicate) {
            if (e1checked && checkPredicate.test(e1)) {
                e1checked = false;
            }

            if (e2checked && checkPredicate.test(e2)) {
                e2checked = false;
            }
        }

        @Override
        public void checkAll() {
            e1checked = true;
            e2checked = true;
        }

        @Override
        public void uncheckAll() {
            e1checked = false;
            e2checked = false;
        }

        @Override
        public boolean isChecked(E element) {
            if (element.equals(e1)) {
                return e1checked;
            } else if (element.equals(e2)) {
                return e2checked;
            } else {
                throw new IllegalArgumentException("Invalid " + elementName + ": " + element);
            }
        }

        @Override
        public boolean isComplete() {
            return e1checked && e2checked;
        }

        @Override
        public boolean isBlank() {
            return !e1checked && !e2checked;
        }

        @Override
        public int size() {
            return 2;
        }

        @Override
        public CheckList<E> getView(Predicate<E> predicate) {
            if (predicate.test(e1)) {
                if (predicate.test(e2)) {
                    return this;
                } else {
                    return new View<>(e1, this);
                }
            } else if (predicate.test(e2)) {
                return new View<>(e2, this);
            } else {
                return empty();
            }
        }

        @Override
        public CheckList<E> getView(ImmutableSet<E> elementSubSet) {
            return getView((e) -> elementSubSet.contains(e));
        }

        @Override
        public ImmutableSet<E> getElements() {
            if (elements == null) {
                elements = ImmutableSet.of(e1, e2);
            }
            return elements;
        }

        @Override
        public ImmutableSet<E> getCheckedElements() {
            if (e1checked) {
                if (e2checked) {
                    return getElements();
                } else {
                    return ImmutableSet.of(e1);
                }
            } else if (e2checked) {
                return ImmutableSet.of(e2);
            } else {
                return ImmutableSet.empty();
            }
        }

        @Override
        public ImmutableSet<E> getUncheckedElements() {
            if (e1checked) {
                if (e2checked) {
                    return ImmutableSet.empty();
                } else {
                    return ImmutableSet.of(e2);
                }
            } else if (e2checked) {
                return ImmutableSet.of(e1);
            } else {
                return getElements();
            }
        }

        static class View<E> implements CheckList<E> {
            private final E e;
            private final TwoElementCheckList<E> delegate;

            View(E e, TwoElementCheckList<E> delegate) {
                this.e = e;
                this.delegate = delegate;
            }

            @Override
            public boolean check(E element) {
                if (element.equals(e)) {
                    delegate.check(element);
                    return true;
                } else {
                    throw new IllegalArgumentException("Invalid " + delegate.elementName + ": " + element);
                }
            }

            @Override
            public void uncheck(E element) {
                if (element.equals(e)) {
                    delegate.uncheck(element);
                } else {
                    throw new IllegalArgumentException("Invalid " + delegate.elementName + ": " + element);
                }
            }

            @Override
            public void uncheckIfPresent(E element) {
                if (element.equals(e)) {
                    delegate.uncheck(element);
                }
            }

            @Override
            public boolean checkIf(Predicate<E> checkPredicate) {
                if (checkPredicate.test(e)) {
                    return delegate.check(e);
                } else {
                    return isComplete();
                }
            }

            @Override
            public void uncheckIf(Predicate<E> checkPredicate) {
                if (checkPredicate.test(e)) {
                    delegate.uncheck(e);
                }
            }

            @Override
            public void checkAll() {
                delegate.check(e);
            }

            @Override
            public void uncheckAll() {
                delegate.uncheck(e);
            }

            @Override
            public boolean isChecked(E element) {
                if (e.equals(element)) {
                    return delegate.isChecked(element);
                } else {
                    throw new IllegalArgumentException("Invalid " + delegate.elementName + ": " + element);
                }
            }

            @Override
            public boolean isComplete() {
                return delegate.isChecked(e);
            }

            @Override
            public boolean isBlank() {
                return !delegate.isChecked(e);
            }

            @Override
            public int size() {
                return 1;
            }

            @Override
            public CheckList<E> getView(Predicate<E> predicate) {
                if (predicate.test(e)) {
                    return this;
                } else {
                    return empty();
                }
            }

            @Override
            public CheckList<E> getView(ImmutableSet<E> elementSubSet) {
                if (elementSubSet.contains(e)) {
                    return this;
                } else {
                    return empty();
                }
            }

            @Override
            public ImmutableSet<E> getElements() {
                return ImmutableSet.of(e);
            }

            @Override
            public ImmutableSet<E> getCheckedElements() {
                if (delegate.isChecked(e)) {
                    return getElements();
                } else {
                    return ImmutableSet.empty();
                }
            }

            @Override
            public ImmutableSet<E> getUncheckedElements() {
                if (!delegate.isChecked(e)) {
                    return getElements();
                } else {
                    return ImmutableSet.empty();
                }
            }

        }

    }

    static class BasicCheckList<E> implements CheckList<E> {

        private final ImmutableSet<E> elements;
        private final ImmutableSet.Builder<E> unchecked;
        private final String elementName;
        private int uncheckedCount;
        private int size;

        BasicCheckList(Set<E> elements, String elementName) {
            this.elements = ImmutableSet.of(elements);
            this.unchecked = new ImmutableSet.Builder<>(this.elements);
            this.size = this.elements.size();
            this.uncheckedCount = this.size;
            this.elementName = elementName;
        }

        @Override
        public boolean check(E element) {
            verifyElement(element);

            doCheck(element);

            return this.uncheckedCount == 0;
        }

        private void doCheck(E element) {
            if (this.unchecked.contains(element)) {
                this.unchecked.remove(element);
                this.uncheckedCount--;
            }
        }

        @Override
        public void uncheck(E element) {
            verifyElement(element);

            doUncheck(element);
        }

        @Override
        public void uncheckIfPresent(E element) {
            if (this.elements.contains(element)) {
                doUncheck(element);
            }
        }

        private void doUncheck(E element) {

            if (!this.unchecked.contains(element)) {
                this.unchecked.with(element);
                this.uncheckedCount++;
            }
        }

        @Override
        public boolean checkIf(Predicate<E> checkPredicate) {
            Iterator<E> iter = this.unchecked.iterator();

            while (iter.hasNext()) {
                if (checkPredicate.test(iter.next())) {
                    iter.remove();
                    this.uncheckedCount--;
                }
            }

            return this.uncheckedCount == 0;
        }

        @Override
        public void uncheckIf(Predicate<E> checkPredicate) {
            for (E element : this.elements) {
                if (!unchecked.contains(element) && checkPredicate.test(element)) {
                    unchecked.with(element);
                    this.uncheckedCount++;
                }
            }
        }

        @Override
        public void checkAll() {
            unchecked.clear();
            this.uncheckedCount = 0;
        }

        @Override
        public void uncheckAll() {
            unchecked.with(elements);
            this.uncheckedCount = size;
        }

        @Override
        public boolean isChecked(E element) {
            verifyElement(element);

            return !this.unchecked.contains(element);
        }

        @Override
        public boolean isComplete() {
            return this.uncheckedCount == 0;
        }

        @Override
        public boolean isBlank() {
            return this.uncheckedCount == this.size;
        }

        @Override
        public int size() {
            return this.size;
        }

        private void verifyElement(E element) {
            if (!elements.contains(element)) {
                throw new IllegalArgumentException("Invalid " + elementName + ": " + element);
            }
        }

        @Override
        public CheckList<E> getView(Predicate<E> predicate) {
            return getView(this.elements.matching(predicate));
        }

        @Override
        public CheckList<E> getView(ImmutableSet<E> elementSubSet) {
            int size = elementSubSet.size();

            if (size == this.elements.size()) {
                return this;
            } else if (size == 0) {
                return empty();
            } else {
                return new View<E>(this, elementSubSet);
            }
        }

        @Override
        public ImmutableSet<E> getElements() {
            return elements;
        }

        @Override
        public ImmutableSet<E> getCheckedElements() {
            if (isComplete()) {
                return elements;
            } else if (isBlank()) {
                return ImmutableSet.empty();
            } else {
                return elements.matching((e) -> !this.unchecked.contains(e));
            }
        }

        @Override
        public ImmutableSet<E> getUncheckedElements() {
            if (isComplete()) {
                return ImmutableSet.empty();
            } else if (isBlank()) {
                return elements;
            } else {
                return elements.matching((e) -> this.unchecked.contains(e));
            }
        }

        static class View<E> implements CheckList<E> {
            private final ImmutableSet<E> elements;
            private final BasicCheckList<E> delegate;

            View(BasicCheckList<E> delegate, ImmutableSet<E> elements) {
                this.delegate = delegate;
                this.elements = elements;
            }

            @Override
            public boolean check(E element) {
                verifyElement(element);

                delegate.doCheck(element);

                return isComplete();
            }

            @Override
            public void uncheck(E element) {
                verifyElement(element);

                delegate.doUncheck(element);
            }

            @Override
            public void uncheckIfPresent(E element) {
                if (elements.contains(element)) {
                    delegate.doUncheck(element);
                }
            }

            @Override
            public boolean checkIf(Predicate<E> checkPredicate) {
                if (delegate.checkIf((e) -> elements.contains(e) && checkPredicate.test(e))) {
                    return true;
                }

                return isComplete();
            }

            @Override
            public void uncheckIf(Predicate<E> checkPredicate) {
                delegate.uncheckIf((e) -> elements.contains(e) && checkPredicate.test(e));
            }

            @Override
            public void checkAll() {
                for (E element : elements) {
                    delegate.doCheck(element);
                }
            }

            @Override
            public void uncheckAll() {
                for (E element : elements) {
                    delegate.doUncheck(element);
                }
            }

            @Override
            public boolean isChecked(E element) {
                verifyElement(element);

                return delegate.isChecked(element);
            }

            @Override
            public boolean isComplete() {
                if (delegate.unchecked.containsAny(elements)) {
                    return false;
                } else {
                    return true;
                }
            }

            @Override
            public boolean isBlank() {
                if (delegate.unchecked.containsAll(elements)) {
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public int size() {
                return elements.size();
            }

            @Override
            public CheckList<E> getView(Predicate<E> predicate) {
                return getView(this.elements.matching(predicate));
            }

            @Override
            public CheckList<E> getView(ImmutableSet<E> elementSubSet) {
                int size = elementSubSet.size();

                if (size == this.elements.size()) {
                    return this;
                } else if (size == 0) {
                    return empty();
                } else {
                    return new View<E>(this.delegate, elementSubSet);
                }
            }

            private void verifyElement(E element) {
                if (!elements.contains(element)) {
                    throw new IllegalArgumentException("Invalid " + delegate.elementName + ": " + element);
                }
            }

            @Override
            public ImmutableSet<E> getElements() {
                return elements;
            }

            @Override
            public ImmutableSet<E> getCheckedElements() {
                if (isComplete()) {
                    return elements;
                } else if (isBlank()) {
                    return ImmutableSet.empty();
                } else {
                    return elements.matching((e) -> !delegate.unchecked.contains(e));
                }
            }

            @Override
            public ImmutableSet<E> getUncheckedElements() {
                if (isComplete()) {
                    return ImmutableSet.empty();
                } else if (isBlank()) {
                    return elements;
                } else {
                    return elements.matching((e) -> delegate.unchecked.contains(e));
                }
            }

        }

    }

    static final CheckList<?> EMPTY = new CheckList<Object>() {

        @Override
        public boolean check(Object element) {
            return true;
        }

        @Override
        public void uncheck(Object element) {

        }

        @Override
        public boolean checkIf(Predicate<Object> checkPredicate) {
            return true;

        }

        @Override
        public void uncheckIf(Predicate<Object> checkPredicate) {

        }

        @Override
        public void checkAll() {

        }

        @Override
        public void uncheckAll() {

        }

        @Override
        public boolean isChecked(Object column) {
            return false;
        }

        @Override
        public boolean isComplete() {
            return true;
        }

        @Override
        public boolean isBlank() {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public CheckList<Object> getView(Predicate<Object> predicate) {
            return this;
        }

        @Override
        public ImmutableSet<Object> getElements() {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<Object> getCheckedElements() {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<Object> getUncheckedElements() {
            return ImmutableSet.empty();

        }

        @Override
        public CheckList<Object> getView(ImmutableSet<Object> elementSubSet) {
            return this;
        }

        @Override
        public void uncheckIfPresent(Object element) {

        }

    };

}
