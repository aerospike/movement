/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.core.util;

import com.aerospike.movement.util.core.stream.mechanics.CyclicStream;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class CyclicStreamTest {


    @Test
    public void testStreamWillRepeat() {
        final AtomicBoolean shouldHalt = new AtomicBoolean(false);
        final AtomicInteger counter = new AtomicInteger(0);
        final CyclicStream<Integer> wheel = CyclicStream.from(
                () -> IntStream.range(0, 3).boxed(), //stream supplier
                () -> shouldHalt.get());             //stream halt check

        wheel.stream().forEach(ele -> {
            counter.incrementAndGet();
            if (counter.get() == 5)
                shouldHalt.set(true);
        });
        assert counter.get() == 5;
        assertEquals(1, wheel.completedRotations());
    }

    @Test
    public void testRotationCounter() {
        final AtomicBoolean shouldHalt = new AtomicBoolean(false);
        final AtomicInteger counter = new AtomicInteger(0);
        final CyclicStream<Integer> wheel = CyclicStream.from(() -> IntStream.range(0, 3).boxed(), () -> shouldHalt.get());
        wheel.stream().forEach(ele -> {
            counter.incrementAndGet();
            if (counter.get() == 2)
                shouldHalt.set(true);
        });
        assert counter.get() == 2;
        assertEquals(0, wheel.completedRotations());
    }
}
