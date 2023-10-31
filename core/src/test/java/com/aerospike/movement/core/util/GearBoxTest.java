/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.core.util;

import com.aerospike.movement.util.core.stream.mechanics.GearBox;
import com.aerospike.movement.util.core.stream.mechanics.PinionSystem;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GearBoxTest {


    @Test
    public void testTrivialGearBox() {
        Supplier<Stream<Integer>> streamSupplierA = () -> IntStream.range(0, 3).boxed();
        Supplier<Stream<Integer>> streamSupplierB = () -> IntStream.range(0, 3).boxed();
        BiFunction<Integer, Integer, Integer> zipFunction = Integer::sum;

        PinionSystem<Integer, Integer, Integer> pinionSystem = PinionSystem.of(streamSupplierA, streamSupplierB, zipFunction, PinionSystem::oneRotationHaltCheck);

        GearBox<Integer, Integer, Integer> box = GearBox.from(pinionSystem);

        List<Integer> results = box.stream()
                .collect(Collectors.toList());
        assertEquals(List.of(0, 2, 4), results);
    }

    @Test
    public void testSimpleGearBox() {
        BiFunction<String, String, String> zipFunction = (s, s2) -> s + ":" + s2;
        List<PinionSystem> pinions = new ArrayList<>();

        Supplier<Stream<String>> streamSupplierA1 = () -> List.of("a", "the", "one").stream();
        Supplier<Stream<String>> streamSupplierB1 = () -> List.of("cat", "dog", "stone").stream();
        pinions.add(PinionSystem.of(streamSupplierA1, streamSupplierB1, zipFunction, PinionSystem::oneRotationHaltCheck));

        Supplier<Stream<String>> streamSupplierA2 = () -> List.of("green", "red", "yellow").stream();
        Supplier<Stream<String>> streamSupplierB2 = () -> List.of("paper", "chair", "tree").stream();
        pinions.add(PinionSystem.of(streamSupplierA2, streamSupplierB2, zipFunction, PinionSystem::oneRotationHaltCheck));


        GearBox<String, String, String> box = GearBox.from(pinions.toArray(new PinionSystem[0]));

        List<String> results = box.stream()
                .collect(Collectors.toList());
        assertTrue(results.contains("a:cat"));
        assertTrue(results.contains("red:chair"));
        assertTrue(results.contains("yellow:tree"));
        assertEquals(6, results.size());
    }


    @Test
    public void testMixedGearBox() {
        BiFunction<String, String, String> zipFunction = (s, s2) -> s + ":" + s2;
        List<PinionSystem<String, String, String>> pinions = new ArrayList<>();

        Supplier<Stream<String>> streamSupplierA1 = () -> List.of("a", "the", "one", "half").stream();
        Supplier<Stream<String>> streamSupplierB1 = () -> List.of("cat", "dog", "stone").stream();
        pinions.add(PinionSystem.of(streamSupplierA1, streamSupplierB1, zipFunction, PinionSystem::oneRotationHaltCheck));

        Supplier<Stream<String>> streamSupplierA2 = () -> List.of("green", "red", "yellow").stream();
        Supplier<Stream<String>> streamSupplierB2 = () -> List.of("paper", "chair", "tree").stream();
        pinions.add(PinionSystem.of(streamSupplierA2, streamSupplierB2, zipFunction, PinionSystem::oneRotationHaltCheck));


        GearBox<String, String, String> gearBox = GearBox.from(pinions);
        List<String> results = gearBox.stream()
                .collect(Collectors.toList());
        assertTrue(results.contains("a:cat"));
        assertTrue(results.contains("red:chair"));
        assertTrue(results.contains("yellow:tree"));
        assertTrue(results.contains("half:cat"));

        assertEquals(7, results.size());
    }

}
