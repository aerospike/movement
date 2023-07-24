package com.aerospike.movement.util.core.iterator;

import java.util.Iterator;

public class IteratorWrap<T> implements CloseableIterator<T> {
    private final Iterator<T> wrappedIterator;

    public IteratorWrap(Iterator<?> iterator) {
        this.wrappedIterator = (Iterator<T>) iterator;
    }

    @Override
    public boolean hasNext() {
        return wrappedIterator.hasNext();
    }

    @Override
    public T next() {
        return wrappedIterator.next();
    }
}
