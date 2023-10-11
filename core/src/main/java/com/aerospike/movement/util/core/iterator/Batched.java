package com.aerospike.movement.util.core.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class Batched implements Iterator<List<Object>> {
    protected final Iterator<?> iterator;
    protected final int batchSize;

    public Batched(Iterator<?> iterator, int batchSize) {
        this.batchSize = batchSize;
        this.iterator = iterator;
    }

    public static Iterator<List<Object>> consume(final Iterator<Object> objectIterator, final int size) {
        return new BatchedConsumer(objectIterator, size);
    }


    @Override
    public boolean hasNext() {
        synchronized (iterator){
            return iterator.hasNext();
        }
    }


    private static class BatchedConsumer extends Batched {
        private BatchedConsumer(Iterator<Object> iterator, int batchSize) {
            super(iterator, batchSize);
        }

        @Override
        public List<Object> next() {
            synchronized (this) {
                final List<Object> results = new ArrayList<>();
                while (iterator.hasNext() && results.size() < batchSize) {
                    results.add(iterator.next());
                }
                return results;
            }
        }
    }
}
