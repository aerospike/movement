/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.stream.sequence;

import com.aerospike.movement.runtime.core.Handler;
import com.aerospike.movement.util.core.error.ErrorUtil;
import com.aerospike.movement.util.core.iterator.OneShotIteratorSupplier;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class SequenceUtil {
    public static Optional<?> takeNext(final Object nextable) {
        if (Iterator.class.isAssignableFrom(nextable.getClass())) {
            return ((Iterator<?>) nextable).hasNext() ? Optional.of(((Iterator<?>) nextable).next()) : Optional.empty();
        } else if (PotentialSequence.class.isAssignableFrom(nextable.getClass())) {
            return ((PotentialSequence<?>) nextable).getNext();
        } else {
            throw new RuntimeException(nextable.getClass() + "is not supported by StreamUtil.takeNext");
        }
    }

    public static <T> PotentialSequence<T> fuse(final OneShotIteratorSupplier<T> iteratorSupplier) {
        return fuse(iteratorSupplier, (e, context) -> {
            throw new RuntimeException(e);
        });
    }

    public static <T> PotentialSequence<T> fuse(final OneShotIteratorSupplier<T> iteratorSupplier, final Handler<Throwable> errorHandler) {
        return new PotentialSequence<T>() {
            private final Iterator<T> iterator = iteratorSupplier.get();

            @Override
            public Optional<T> getNext() {
                synchronized (iterator) {
                    try {
                        if (!iterator.hasNext()) {
                            return Optional.empty();
                        } else {
                            return Optional.of(iterator.next());
                        }
                    } catch (final Exception e) {
                        errorHandler.handle(e, this, iterator);
                        return Optional.empty();
                    }
                }
            }
        };
    }
}
