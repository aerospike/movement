/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.stream.sequence;

import com.aerospike.movement.util.core.iterator.OneShotIteratorSupplier;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface PotentialSequence<T> {
    Optional<T> getNext();


    default Stream<Optional<T>> stream(final BiFunction<PotentialSequence<T>, Optional<T>, Boolean> haltChecker) {
        return Stream.iterate(getNext(), it -> haltChecker.apply(this, it), it -> getNext());
    }

    /*
    by default halt if we get a non value
     */
    default Stream<Optional<T>> stream() {
        return stream((seq, ele) -> ele.isEmpty());
    }

}


