/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */
package com.aerospike.movement.util.core.iterator.ext;

import java.io.Closeable;
import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

    /**
     * Wraps an existing {@code Iterator} in a {@code CloseableIterator}. If the {@code Iterator} is already of that
     * type then it will simply be returned as-is.
     */
    static <T> CloseableIterator<T> of(final Iterator<T> iterator) {
        if (iterator instanceof CloseableIterator)
            return (CloseableIterator<T>) iterator;

        return new DefaultCloseableIterator<T>(iterator);
    }

    @Override
    default void close() {
        // do nothing by default
    }

    static <T> void closeIterator(final Iterator<T> iterator) {
        if (iterator instanceof AutoCloseable) {
            try {
                ((AutoCloseable) iterator).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    static <T> CloseableIterator<T> empty() {
        return EmptyCloseableIterator.instance();
    }

    class EmptyCloseableIterator<T> extends DefaultCloseableIterator<T> {

        private static final EmptyCloseableIterator INSTANCE = new EmptyCloseableIterator();

        public static <T> EmptyCloseableIterator<T> instance() {
            return INSTANCE;
        }

        private EmptyCloseableIterator() {
            super(EmptyIterator.instance());
        }

    }
}
