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
final class SingleIterator<T> implements Iterator<T>, Serializable {

    private T t;
    private boolean alive = true;

    protected SingleIterator(final T t) {
        this.t = t;
    }

    @Override
    public boolean hasNext() {
        return this.alive;
    }

    @Override
    public void remove() {
        this.t = null;
    }

    @Override
    public T next() {
        if (!this.alive)
            throw FastNoSuchElementException.instance();
        else {
            this.alive = false;
            return t;
        }
    }
}
