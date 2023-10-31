/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.iterator;

import com.aerospike.movement.util.core.iterator.ext.CloseableIterator;

import java.util.Iterator;

public class IteratorWrap<T> implements CloseableIterator<T> {
    private final Iterator<T> wrappedIterator;

    public IteratorWrap(Iterator<?> iterator) {
        this.wrappedIterator = (Iterator<T>) iterator;
    }

    @Override
    public boolean hasNext() {
        return wrappedIterator.hasNext();
    }

    @Override
    public T next() {
        return wrappedIterator.next();
    }
}
