package com.aerospike.movement.util.core.iterator;

import java.util.Iterator;

public interface IteratorSupplier {
    Iterator<Object> get();
}
