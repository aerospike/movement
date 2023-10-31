/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */
package com.aerospike.movement.util.core.iterator.ext;

import com.aerospike.movement.util.core.error.exception.FastNoSuchElementException;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class DoubleIterator<T> implements Iterator<T>, Serializable {

    private T a;
    private T b;
    private char current = 'a';

    protected DoubleIterator(final T a, final T b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean hasNext() {
        return this.current != 'x';
    }

    @Override
    public void remove() {
        if (this.current == 'b')
            this.a = null;
        else if (this.current == 'x')
            this.b = null;
    }

    @Override
    public T next() {
        if (this.current == 'x')
            throw FastNoSuchElementException.instance();
        else {
            if (this.current == 'a') {
                this.current = 'b';
                return this.a;
            } else {
                this.current = 'x';
                return this.b;
            }
        }
    }
}
