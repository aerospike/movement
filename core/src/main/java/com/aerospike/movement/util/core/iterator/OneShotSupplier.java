package com.aerospike.movement.util.core.iterator;

import com.aerospike.movement.util.core.CheckedNotThreadSafe;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class OneShotSupplier extends CheckedNotThreadSafe implements IteratorSupplier{
    private final IteratorSupplier supplier;
    private AtomicBoolean used = new AtomicBoolean(false);

    protected OneShotSupplier(final IteratorSupplier supplier) {
        this.supplier = supplier;
    }
    public static OneShotSupplier of(final IteratorSupplier supplier){
        return new OneShotSupplier(supplier);
    }

    public Iterator<Object> get() {
        checkThreadAssignment();
        if (used.compareAndSet(false, true)) {
            return supplier.get();
        } else throw new IllegalStateException("IteratorSupplier can only be used once");
    }
}
