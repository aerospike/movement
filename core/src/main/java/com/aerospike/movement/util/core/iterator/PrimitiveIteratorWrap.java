package com.aerospike.movement.util.core.iterator;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

public class PrimitiveIteratorWrap implements Iterator<Object> {
    private final PrimitiveIterator.OfLong primitiveIterator;

    private PrimitiveIteratorWrap(PrimitiveIterator.OfLong longIterator) {
        this.primitiveIterator = longIterator;
    }

    public static PrimitiveIteratorWrap wrap(PrimitiveIterator.OfLong longIterator) {
        return new PrimitiveIteratorWrap(longIterator);
    }

    public static PrimitiveIteratorWrap wrap(LongStream stream) {
        return new PrimitiveIteratorWrap(stream.iterator());
    }

    @Override
    public boolean hasNext() {
        return primitiveIterator.hasNext();
    }

    @Override
    public Object next() {
        return Long.valueOf(primitiveIterator.next());
    }
}
