package com.aerospike.graph.move.util;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BatchedIterator<T> implements Iterator<T> {
    private final Iterator<List<T>> upstreamIterator;
    private Iterator<T> bufferedIterator;
    private List<T> buffer;

    public BatchedIterator(Iterator<List<T>> iterator) {
        this.upstreamIterator = iterator;
        fillBuffer();
    }

    private boolean fillBuffer() {
        if (upstreamIterator.hasNext()) {
            buffer = upstreamIterator.next();
        }
        this.bufferedIterator = buffer.iterator();
        return bufferedIterator.hasNext();
    }

    @Override
    public boolean hasNext() {
        if (bufferedIterator.hasNext()) {
            return true;
        }
        return fillBuffer();
    }

    @Override
    public T next() {
        if (hasNext()) {
            return bufferedIterator.next();
        } else {
            throw new NoSuchElementException();
        }
    }
}
