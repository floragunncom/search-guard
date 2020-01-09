package com.floragunn.searchsupport.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Iterators;

public class SingleElementBlockingQueue<E> implements BlockingQueue<E> {

    private final AtomicReference<E> element = new AtomicReference<>();
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();

    @Override
    public E remove() {
        E result = element.getAndSet(null);

        if (result == null) {
            throw new NoSuchElementException();
        }

        return result;
    }

    @Override
    public E poll() {
        return element.getAndSet(null);
    }

    @Override
    public E element() {
        E result = element.get();

        if (result == null) {
            throw new NoSuchElementException();
        }

        return result;
    }

    @Override
    public E peek() {
        return element.get();
    }

    @Override
    public int size() {
        return element.get() != null ? 1 : 0;
    }

    @Override
    public boolean isEmpty() {
        return element.get() == null;

    }

    @Override
    public Iterator<E> iterator() {
        E e = element.get();

        if (e == null) {
            return Collections.emptyIterator();
        } else {
            return Collections.singletonList(e).iterator();
        }
    }

    @Override
    public Object[] toArray() {
        E e = element.get();

        if (e == null) {
            return new Object[0];
        } else {
            return new Object[] { e };
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] toArray(T[] a) {
        E e = element.get();

        int size = e != null ? 1 : 0;

        if (a.length < size) {
            a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
        }

        if (e != null) {
            a[0] = (T) e;
        }

        return a;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        E e = element.get();

        if (e == null) {
            return c.size() == 0;
        }

        for (Object o : c) {
            if (o != e) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        E last = Iterators.getLast(c.iterator());
        return add(last);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean changed = false;
        for (Object e : c) {
            changed |= remove(e);
        }
        return changed;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        E e = element.get();
        boolean contains = false;

        for (Object o : c) {
            if (o == e) {
                contains = true;
            }
        }

        if (!contains) {
            return element.compareAndSet(e, null);
        } else {
            return false;
        }
    }

    @Override
    public void clear() {
        element.set(null);
    }

    @Override
    public boolean add(E e) {

        if (e == null) {
            throw new IllegalArgumentException("null not allowed");
        }
        element.set(e);
        notEmpty.signal();

        return true;
    }

    @Override
    public boolean offer(E e) {
        element.set(e);
        return true;
    }

    @Override
    public void put(E e) throws InterruptedException {
        element.set(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        element.set(e);
        return true;
    }

    @Override
    public E take() throws InterruptedException {

        E e = element.getAndSet(null);

        if (e != null) {
            return e;
        }

        takeLock.lockInterruptibly();
        try {
            e = element.getAndSet(null);

            if (e != null) {
                return e;
            }

            while (e == null) {
                notEmpty.await();
                e = element.getAndSet(null);
            }

            return e;
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {

        E e = element.getAndSet(null);

        if (e != null) {
            return e;
        }

        takeLock.lockInterruptibly();
        try {
            e = element.getAndSet(null);

            if (e != null) {
                return e;
            }

            while (e == null) {
                notEmpty.await(timeout, unit);
                e = element.getAndSet(null);
            }

            return e;
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        E e = element.get();

        return e == null ? 1 : 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object o) {
        return element.compareAndSet((E) o, null);
    }

    @Override
    public boolean contains(Object o) {
        E e = element.get();
        return e == o;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        E e = element.getAndSet(null);

        if (e != null) {
            c.add(e);
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        if (maxElements <= 0) {
            return 0;
        } else {
            return drainTo(c);
        }
    }
}
