package com.aerospike.graph.generator.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SyncronizedBatchIterator<T> implements Iterator<List<T>> {
    private final Iterator<T> iterator;
    private final int batchSize;

    public SyncronizedBatchIterator(Iterator<T> iterator, int batchSize) {
        this.batchSize = batchSize;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        synchronized (this) {
            return iterator.hasNext();
        }
    }

    @Override
    public List<T> next() {
        synchronized (this) {
            final List<T> results = new ArrayList<>();
            while (iterator.hasNext() && results.size() < batchSize) {
                results.add(iterator.next());
            }
            return results;
        }
    }
}
