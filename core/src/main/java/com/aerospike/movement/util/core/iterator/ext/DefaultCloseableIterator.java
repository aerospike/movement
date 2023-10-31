/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */
package com.aerospike.movement.util.core.iterator.ext;

import com.aerospike.movement.util.core.iterator.ext.CloseableIterator;

import java.util.Iterator;

/**
 * A default implementation of {@link CloseableIterator} that simply wraps an existing {@code Iterator}. This
 * implementation has a "do nothing" implementation of {@link #close()}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultCloseableIterator<T> implements CloseableIterator<T> {
    protected Iterator<T> iterator;

    public DefaultCloseableIterator(final Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }

    @Override
    public void close() {
        CloseableIterator.closeIterator(iterator);
    }
}
