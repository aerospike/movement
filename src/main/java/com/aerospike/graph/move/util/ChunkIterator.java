package com.aerospike.graph.move.util;

import java.util.Iterator;
import java.util.stream.LongStream;

public class ChunkIterator implements Iterator<Long> {
    private final long chunkSize;
    private final RangeIterator ri;
    private Iterator<Long> it;

    public ChunkIterator(final RangeIterator ri, final long chunkSize) {
        this.chunkSize = chunkSize;
        this.ri = ri;
        final Long top = ri.next();
        it = LongStream.range(top-chunkSize, top).iterator();
    }

    @Override
    public boolean hasNext() {
        return it.hasNext() || ri.hasNext();
    }

    @Override
    public Long next() {
        if (it.hasNext())
            return it.next();
        else if (ri.hasNext()) {
            final Long top = ri.next();
            it = LongStream.range(top-chunkSize + 1, top).iterator();
        }
        return next();
    }
}
