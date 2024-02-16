/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.iterator;

import com.aerospike.movement.util.core.runtime.CheckedNotThreadSafe;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class OneShotIteratorSupplier<T> extends CheckedNotThreadSafe implements IteratorSupplier<T>{
    private final IteratorSupplier<T> supplier;
    private AtomicBoolean used = new AtomicBoolean(false);

    protected OneShotIteratorSupplier(final IteratorSupplier<T> supplier) {
        this.supplier = supplier;
    }
    public static <T>OneShotIteratorSupplier<T> of(final IteratorSupplier<T> supplier){
        return new OneShotIteratorSupplier<T>(supplier);
    }

    public Iterator<T> get() {
        checkThreadAssignment();
        if (used.compareAndSet(false, true)) {
            return supplier.get();
        } else{
            throw new IllegalStateException("IteratorSupplier can only be used once");
        }
    }
}
