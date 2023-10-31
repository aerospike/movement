/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */
package com.aerospike.movement.util.core.iterator.ext;

import com.aerospike.movement.util.core.error.exception.FastNoSuchElementException;
import com.aerospike.movement.util.core.iterator.ext.CloseableIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MultiIterator<T> implements Iterator<T>, Serializable, AutoCloseable {

    private final List<Iterator<T>> iterators = new ArrayList<>();
    private int current = 0;

    public void addIterator(final Iterator<T> iterator) {
        this.iterators.add(iterator);
    }

    @Override
    public boolean hasNext() {
        if (this.current >= this.iterators.size())
            return false;

        Iterator<T> currentIterator = this.iterators.get(this.current);

        while (true) {
            if (currentIterator.hasNext()) {
                return true;
            } else {
                this.current++;
                if (this.current >= iterators.size())
                    break;
                currentIterator = iterators.get(this.current);
            }
        }
        return false;
    }

    @Override
    public void remove() {
        this.iterators.get(this.current).remove();
    }

    @Override
    public T next() {
        if (this.iterators.isEmpty()) throw FastNoSuchElementException.instance();

        Iterator<T> currentIterator = iterators.get(this.current);
        while (true) {
            if (currentIterator.hasNext()) {
                return currentIterator.next();
            } else {
                this.current++;
                if (this.current >= iterators.size())
                    break;
                currentIterator = iterators.get(current);
            }
        }
        throw FastNoSuchElementException.instance();
    }

    public void clear() {
        this.iterators.clear();
        this.current = 0;
    }

    /**
     * Close the underlying iterators if auto-closeable. Note that when Exception is thrown from any iterator
     * in the for loop on closing, remaining iterators possibly left unclosed.
     */
    @Override
    public void close() {
        for (Iterator<T> iterator : this.iterators) {
            CloseableIterator.closeIterator(iterator);
        }
    }
}
