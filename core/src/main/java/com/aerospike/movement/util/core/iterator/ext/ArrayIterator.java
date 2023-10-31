/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */
package com.aerospike.movement.util.core.iterator.ext;


import com.aerospike.movement.util.core.error.exception.FastNoSuchElementException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ArrayIterator<T> implements Iterator<T>, Serializable {

    private final T[] array;
    private int current = 0;

    public ArrayIterator(final T[] array) {
        this.array = Objects.requireNonNull(array);
    }

    @Override
    public boolean hasNext() {
        return this.current < this.array.length;
    }

    @Override
    public T next() {
        if (this.hasNext()) {
            this.current++;
            return this.array[this.current - 1];
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public String toString() {
        return array.length == 1 && null == array[0] ? "[null]" : Arrays.asList(array).toString();
    }
}
