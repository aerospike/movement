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
public final class EmptyIterator<S> implements Iterator<S>, Serializable {

    private static final EmptyIterator INSTANCE = new EmptyIterator<>();

    private EmptyIterator() {
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public S next() {
        throw FastNoSuchElementException.instance();
    }

    public static <S> Iterator<S> instance() {
        return INSTANCE;
    }
}