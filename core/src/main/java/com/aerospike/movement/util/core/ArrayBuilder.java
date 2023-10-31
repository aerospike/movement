/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core;

import java.util.Optional;

public class ArrayBuilder {
    final Optional<ArrayBuilder> wrapped;
    final Object[] array;

    public static Object[] appendToArray(Object[] array, Object... obj) {
        if (Optional.ofNullable(obj).isEmpty())
            return array;
        final Object[] ret = new Object[array.length + obj.length];
        System.arraycopy(array, 0, ret, 0, array.length);
        System.arraycopy(obj, 0, ret, array.length, obj.length);
        return ret;
    }

    public static Object[] prependToArray(Object[] array, Object... obj) {
        if (Optional.ofNullable(obj).isEmpty())
            return array;
        Object[] ret = new Object[array.length + obj.length];
        System.arraycopy(array, 0, ret, obj.length, array.length);
        System.arraycopy(obj, 0, ret, 0, obj.length);
        return ret;
    }

    private ArrayBuilder(final ArrayBuilder wrappedInstance, final Object... array) {
        this.wrapped = Optional.ofNullable(wrappedInstance);
        this.array = Optional.ofNullable(array).or(() -> Optional.of(new Object[0])).get();
    }

    public static ArrayBuilder create(Object... array) {
        return new ArrayBuilder(null, array);
    }

    public ArrayBuilder addToFront(Object... obj) {
        return new ArrayBuilder(this, prependToArray(array, obj));

    }

    public ArrayBuilder addToBack(Object... obj) {
        return new ArrayBuilder(this, appendToArray(array, obj));
    }

    public Object[] build() {
        if (wrapped.isPresent()) {
            return appendToArray(wrapped.get().build(), array);
        } else {
            return array;
        }
    }
}
