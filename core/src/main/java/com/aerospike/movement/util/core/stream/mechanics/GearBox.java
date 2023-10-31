/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.stream.mechanics;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * GearBox will take a list of pinion systems and run them all,
 * creating one output stream.
 * The GearBox stream will halt when all the contained PinionSystems return the Empty Stream.
 */

public class GearBox<A, B, Z> {
    private final List<PinionSystem<A, B, Z>> pinions;

    public GearBox(final List<PinionSystem<A, B, Z>> pinions) {
        this.pinions = pinions;
    }

    public List<PinionSystem> runningPinions() {
        return pinions.stream().filter(it -> !it.isComplete()).collect(Collectors.toList());
    }

    public List<PinionSystem> finishedPinions() {
        return pinions.stream().filter(it -> !it.isComplete()).collect(Collectors.toList());
    }

    public boolean isComplete() {
        return pinions.stream().allMatch(PinionSystem::isComplete);
    }

    public static <A, B, Z> GearBox<A, B, Z> from(final List<PinionSystem<A, B, Z>> pinions) {
        return new GearBox<>(pinions);
    }

    public static <A, B, Z> GearBox<A, B, Z> from(final PinionSystem<A, B, Z>... pinions) {
        return from(Arrays.asList(pinions));
    }

    public Stream<Z> stream() {
        return pinions.stream()
                .flatMap(pinionSystem ->
                        Stream.iterate(pinionSystem.getNext(), Optional::isPresent, i -> pinionSystem.getNext()))
                .filter(Optional::isPresent)
                .flatMap(Optional::get);
    }
}
