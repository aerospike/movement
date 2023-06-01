package com.aerospike.graph.generator.util;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class RangeIterator implements Iterator<Long> {
    static final AtomicLong current = new AtomicLong(0);
    private final long chunkSize;

    public RangeIterator(long chunkSize){
        this.chunkSize = chunkSize;
    }
    public void setBase(long base){
        if(current.get() == 0)
            current.set(base);
        else throw new IllegalStateException("Base already set");
    }
    
    @Override
    public boolean hasNext() {
        return current.get() < Long.MAX_VALUE - chunkSize;
    }

    @Override
    public Long next() {
        return current.addAndGet(chunkSize);
    }
}
