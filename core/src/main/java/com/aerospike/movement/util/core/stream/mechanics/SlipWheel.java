/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.stream.mechanics;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.aerospike.movement.util.core.stream.mechanics.PinionSystem.incrementOdometer;

public class SlipWheel<A, B, Z> implements BiFunction<A,Iterator<B>,Stream<Z>>{
    private final long notchesToSlip;
    private final BiFunction<A, B, Z> zipFunction;
    private final PinionSystem<A, B, Z> pinionSystem;

    public SlipWheel(final long notchesToSlip, final BiFunction<A, B, Z> zipFunction, final PinionSystem<A, B, Z> pinionSystem) {
        this.notchesToSlip = notchesToSlip;
        this.zipFunction = zipFunction;
        this.pinionSystem = pinionSystem;
    }

    public static <A, B, Z> SlipWheel<A, B, Z> with(final long notchesToSlip, final BiFunction<A, B, Z> zipFunction, final PinionSystem<A, B, Z> pinionSystem) {
        return new SlipWheel<>(notchesToSlip, zipFunction, pinionSystem);
    }

    public Stream<Z> apply(final A a, final Iterator<B> bIterator) {
        if (!bIterator.hasNext())
            return Stream.empty();
        return Stream.iterate(0,
                        i -> i < notchesToSlip && bIterator.hasNext(),
                        i -> i + 1)
                .map(notch -> zipFunction.apply(a, incrementOdometer(pinionSystem.odometerB, bIterator)));
    }
}
