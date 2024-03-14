/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.core.util;

import com.aerospike.movement.util.core.stream.mechanics.PinionSystem;
import com.aerospike.movement.util.core.stream.mechanics.ZipFunction;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class PinionSystemTest {


    @Test
    public void testPairOfStreamsWillHaltInSync() {
        Supplier<Stream<Integer>> streamSupplierA = () -> IntStream.range(0, 3).boxed();
        Supplier<Stream<Integer>> streamSupplierB = () -> IntStream.range(0, 3).boxed();

        PinionSystem<Integer, Integer, Integer> pinionSystem = PinionSystem.of(streamSupplierA, streamSupplierB, ZipFunction.zipAddition, PinionSystem::oneRotationHaltCheck);
        List<Integer> results = Stream.iterate(pinionSystem.getNext(), Optional::isPresent, i -> pinionSystem.getNext())
                .filter(it -> {
                    return it.isPresent();
                })
                .flatMap(it -> {
                    final Stream<Integer> x = it.get();
                    return x;
                })
                .collect(Collectors.toList());

        assertEquals(List.of(0, 2, 4), results);
    }

    @Test
    public void testCustomHaltFunction() {
        Supplier<Stream<Integer>> streamSupplierA = () -> IntStream.range(0, 3).boxed();
        Supplier<Stream<Integer>> streamSupplierB = () -> IntStream.range(0, 3).boxed();

        //2 full rotations
        final PinionSystem<Integer, Integer, Integer> pinionSystem = PinionSystem.of(streamSupplierA, streamSupplierB, ZipFunction.zipAddition,
                (a, b) -> {
                    if (a.startCounter.get() >= 2 && b.startCounter.get() >= 2)
                        return true;
                    return false;
                });

        List<Integer> results = Stream.iterate(pinionSystem.getNext(), Optional::isPresent, i -> pinionSystem.getNext())
                .filter(Optional::isPresent)
                .flatMap(Optional::get)
                .collect(Collectors.toList());

        assertEquals(List.of(0, 2, 4, 0, 2, 4), results);
    }


    @Test
    public void differentSizeGears() {
        //rotate 2 different size gears together
        Supplier<Stream<Integer>> streamSupplierA = () -> IntStream.range(0, 6).boxed();
        Supplier<Stream<Integer>> streamSupplierB = () -> IntStream.range(0, 3).boxed();

        PinionSystem<Integer, Integer, Integer> pinionSystem = PinionSystem.of(streamSupplierA, streamSupplierB, ZipFunction.zipAddition, PinionSystem::oneRotationHaltCheck);
        List<Integer> results = Stream.iterate(pinionSystem.getNext(), Optional::isPresent, i -> pinionSystem.getNext())
                .filter(Optional::isPresent)
                .flatMap(Optional::get)
                .collect(Collectors.toList());

        assertEquals(List.of(0, 2, 4, 3, 5, 7), results);
    }

    @Test
    public void differentSizeGearsPartialRotation() {
        //rotate 2 different size gears together
        Supplier<Stream<Integer>> streamSupplierA = () -> IntStream.range(0, 7).boxed();
        Supplier<Stream<Integer>> streamSupplierB = () -> IntStream.range(0, 3).boxed();
        PinionSystem<Integer, Integer, Integer> pinionSystem = PinionSystem.of(streamSupplierA, streamSupplierB, ZipFunction.zipAddition, PinionSystem::oneRotationHaltCheck);
        List<Integer> results = Stream.iterate(pinionSystem.getNext(), Optional::isPresent, i -> pinionSystem.getNext())
                .filter(Optional::isPresent)
                .flatMap(Optional::get)
                .collect(Collectors.toList());

        assertEquals(List.of(0, 2, 4, 3, 5, 7, 6), results);
    }
}
