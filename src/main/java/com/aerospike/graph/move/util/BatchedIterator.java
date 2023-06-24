package com.aerospike.graph.move.util;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BatchedIterator implements Iterator<List<?>> {
    private final Iterator<List<?>> upstreamIterator;
    private Iterator<?> bufferedIterator;
    private List<?> buffer;

    public BatchedIterator(Iterator<List<?>> iterator) {
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
    public List<?> next() {
        if (hasNext()) {
            return (List<Object>) bufferedIterator.next();
        } else {
            throw new NoSuchElementException();
        }
    }
}
