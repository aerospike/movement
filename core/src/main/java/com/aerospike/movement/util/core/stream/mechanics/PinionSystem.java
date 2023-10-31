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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

/*
    Pinion System will zip 2 streams of different lengths, by treating them as 2 wheels
    the smaller stream will be restarted from the beginning when it comes to an end.
    a provided function will determine when to halt the 2 wheels.
    the default halt check will stop the PinionSystem when both streams
    have completed 1 full cycle.
    */
/*
    A zipperFunction can be configured to act as a "slip wheel"
    this will allow one point on the left hand wheel to encounter multiple points on the right hand wheel.
    A may have an opportunity chance to meet more than one B.
 */
public class PinionSystem<A, B, Z> implements PotentialSequence<Stream<Z>> {


    public enum Gear {
        A, B;


        public AtomicLong counter(final PinionSystem<?, ?, ?> pinionSystem) {
            return this.equals(Gear.A) ? pinionSystem.odometerA : pinionSystem.odometerB;
        }
    }

    public static final boolean counters = true;
    public final AtomicLong odometerA = new AtomicLong(0);
    public final AtomicLong odometerB = new AtomicLong(0);
    public final CyclicStream<A> gearA;
    public final CyclicStream<B> gearB;
    private final SlipWheel<A, B, Z> slipWheel;
    private final Iterator<A> iteratorA;
    private final Iterator<B> iteratorB;
    private final BiFunction<CyclicStream<?>, CyclicStream<?>, Boolean> checkPinionComplete;


    public PinionSystem(final Supplier<Stream<A>> a,
                        final Supplier<Stream<B>> b,
                        final BiFunction<A, B, Z> zipFunction,
                        final BiFunction<CyclicStream<?>, CyclicStream<?>, Boolean> checkPinionComplete) {
        this.checkPinionComplete = checkPinionComplete;
        this.gearA = CyclicStream.from(a, this::isComplete);
        this.gearB = CyclicStream.from(b, this::isComplete);
        this.iteratorA = gearA.stream().iterator();
        this.iteratorB = gearB.stream().iterator();
        this.slipWheel = SlipWheel.with(1, zipFunction,this);

    }


    public static boolean oneRotationHaltCheck(final CyclicStream<?> a, final CyclicStream<?> b) {
        if (a.startCounter.get() >= 1 && b.startCounter.get() >= 1)
            return true;
        return false;
    }

    public boolean isComplete() {
        final Boolean c = checkPinionComplete.apply(gearA, gearB);
        return c;
    }

    public static <A, B, Z> PinionSystem<A, B, Z> of(final Supplier<Stream<A>> a,
                                                     final Supplier<Stream<B>> b,
                                                     final BiFunction<A, B, Z> zipFunction,
                                                     final BiFunction<CyclicStream<?>, CyclicStream<?>, Boolean> checkPinionComplete) {
        return new PinionSystem<>(a, b, zipFunction, checkPinionComplete);
    }


    public static <X> X incrementOdometer(final AtomicLong od, final Iterator<X> wheelIter) {
        if (counters)
            od.incrementAndGet();
        X value = wheelIter.next();
        return value;
    }

    @Override
    public Optional<Stream<Z>> getNext() {
        synchronized (this) {
            if (isComplete()) {
                return Optional.empty();
            }
            if (!iteratorA.hasNext()) {
                return Optional.empty();
            }
            final A nextA = incrementOdometer(odometerA, iteratorA);
            return Optional.of(slipWheel.apply(nextA, iteratorB));
        }
    }

}
