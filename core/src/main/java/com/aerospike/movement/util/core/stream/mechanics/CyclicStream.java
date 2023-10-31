/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.stream.mechanics;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class CyclicStream<T> {
    private final Callable<Boolean> isComplete;
    private final Supplier<Stream<T>> streamSupplier;
    public final AtomicLong startCounter = new AtomicLong(0);


    public CyclicStream(final Supplier<Stream<T>> streamSupplier,
                        final Callable<Boolean> isComplete) {
        this.isComplete = isComplete;
        this.streamSupplier = streamSupplier;
    }


    public long completedRotations() {
        return startCounter.get() - 1;
    }

    public static <T> CyclicStream<T> from(final Supplier<Stream<T>> streamSupplier,
                                           final Callable<Boolean> haltCheck) {
        return new CyclicStream<>(streamSupplier, haltCheck);
    }

    private boolean checkIsComplete() {
        try {
            final Boolean x = isComplete.call();
            return x;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Stream<T> restartStream() {
        startCounter.incrementAndGet();
        final Stream<T> nextStream = streamSupplier.get();
        return nextStream;
    }

    public Stream<T> stream() {
        return Stream.iterate((Stream<T>)Stream.empty(),
                        s -> !checkIsComplete(),
                        s -> restartStream())
                .flatMap(i -> checkIsComplete() ? Stream.empty() : streamSupplier.get())
                .flatMap(ele -> checkIsComplete() ?
                        Stream.empty() :
                        Stream.of(ele));
    }
}
