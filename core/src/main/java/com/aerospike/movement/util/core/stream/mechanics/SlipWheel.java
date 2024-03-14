/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.stream.mechanics;

import com.aerospike.movement.util.core.stream.sequence.PotentialSequence;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.aerospike.movement.util.core.stream.mechanics.PinionSystem.incrementOdometer;

public class SlipWheel<A, B, Z> implements BiFunction<A, PotentialSequence<B>, Stream<Z>> {
    private final long notchesToSlip;
    private final BiFunction<A, B, Optional<Z>> zipFunction;
    private final PinionSystem<A, B, Z> pinionSystem;

    public SlipWheel(final long notchesToSlip, final BiFunction<A, B, Optional<Z>> zipFunction, final PinionSystem<A, B, Z> pinionSystem) {
        this.notchesToSlip = notchesToSlip;
        this.zipFunction = zipFunction;
        this.pinionSystem = pinionSystem;
    }

    public static <A, B, Z> SlipWheel<A, B, Z> with(final long notchesToSlip, final BiFunction<A, B, Optional<Z>> zipFunction, final PinionSystem<A, B, Z> pinionSystem) {
        return new SlipWheel<>(notchesToSlip, zipFunction, pinionSystem);
    }

    @Override
    public Stream<Z> apply(final A a, final PotentialSequence<B> bSeq) {

        return (Stream<Z>) Stream.iterate(0,
                        i -> i < notchesToSlip,
                        i -> i + 1)
                .map(notch -> {
                    final Optional<B> x = incrementOdometer(pinionSystem.odometerB, bSeq);
                    return x.isEmpty() ? Optional.empty() : zipFunction.apply(a, x.get());
                }).filter(Optional::isPresent)
                .map(Optional::get);
    }

}
