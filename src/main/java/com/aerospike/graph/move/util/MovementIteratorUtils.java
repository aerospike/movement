package com.aerospike.graph.move.util;

import com.aerospike.graph.move.runtime.distributed.DistributedStreamRuntime;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

public class MovementIteratorUtils {
    public static Iterator<Long> longIterator(Iterator<Object> idSupplier) {
        return new IteratorWrap<>(idSupplier);
    }

    public static Iterator<Long> wrapToLong(Iterator<Object> iterator) {
        return new IteratorWrap<Long>(iterator);
    }

    public static class IteratorWrap<T> implements Iterator<T> {
        private final Iterator<T> wrappedIterator;

        public IteratorWrap(Iterator<?> iterator) {
            this.wrappedIterator = (Iterator<T>) iterator;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw ErrorUtil.unimplemented();
        }
    }

    public static class PrimitiveIteratorWrap implements Iterator<Object> {
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

    public static class RangeIterator implements Iterator<Long> {
        static final AtomicLong current = new AtomicLong(0);
        private final long chunkSize;

        public RangeIterator(long chunkSize) {
            this.chunkSize = chunkSize;
        }

        public void setBase(long base) {
            if (current.get() == 0)
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

    public static class BatchIterator implements Iterator<List<Object>> {
        private final Iterator<Object> iterator;
        private final int batchSize;

        private BatchIterator(Iterator<Object> iterator, int batchSize) {
            this.batchSize = batchSize;
            this.iterator = iterator;
        }

        public static BatchIterator create(final Iterator<Object> objectIterator, final int size) {
            return new BatchIterator(objectIterator, size);
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public List<Object> next() {
            final List<Object> results = new ArrayList<>();
            while (iterator.hasNext() && results.size() < batchSize) {
                results.add(iterator.next());
            }
            return results;

        }
    }

    public static class SyncronizedBatchIterator implements Iterator<List<Object>> {
        public static SyncronizedBatchIterator create(Iterator<Object> iterator, int batchSize) {
            return new SyncronizedBatchIterator(new BatchIterator(iterator, batchSize));
        }
        private final BatchIterator iterator;

        private SyncronizedBatchIterator(final BatchIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            synchronized (this) {
                return iterator.hasNext();
            }
        }

        @Override
        public List<Object> next() {
            synchronized (this) {
                return iterator.next();
            }
        }
    }


    public static class IteratorWithFeeder implements Iterator<List<Object>> {
        private final DistributedStreamRuntime distributedStreamRuntime;
        final BlockingQueue<List<Long>> feeder = new LinkedBlockingQueue<>();
        private final Vertx vertx;
        private final String idType;
        final AtomicBoolean isStarted = new AtomicBoolean(false);

        public IteratorWithFeeder(DistributedStreamRuntime distributedStreamRuntime, final Vertx vertx, final String idType) {
            this.distributedStreamRuntime = distributedStreamRuntime;
            this.vertx = vertx;
            this.idType = idType;
        }

        public void start() {
            if (!isStarted.compareAndSet(false, true)) {
                vertx.setPeriodic(1000, id -> {
                    int fillSize = 3;
                    if (feeder.isEmpty() || feeder.size() < fillSize) {
                        vertx.eventBus().request(DistributedStreamRuntime.Config.Keys.ID_CHANNEL, new JsonObject().put("type", idType), ar -> {
                            if (ar.succeeded()) {
                                Optional<List<Long>> batch = distributedStreamRuntime.parseIdBatch(ar.result());
                                if (batch.isPresent()) {
                                    feed(batch.get());
                                }
                            } else {
                                distributedStreamRuntime.handleError(ar.cause());
                            }
                        });
                    }
                });
            }
        }

        public void feed(List<Long> buffer) {
            feeder.add(buffer);
        }

        @Override
        public boolean hasNext() {
            return !feeder.isEmpty();
        }

        @Override
        public List<Object> next() {
            try {
                return new ArrayList<>(feeder.take());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
