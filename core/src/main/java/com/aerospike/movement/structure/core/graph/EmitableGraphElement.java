/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.structure.core.graph;

import com.aerospike.movement.emitter.core.Emitable;

import java.util.Optional;
import java.util.stream.Stream;


public interface EmitableGraphElement extends Emitable {

    Stream<String> propertyNames();

    Optional<Object> propertyValue(String name);

    String label();

    Stream<Emitable> stream();

    default String type() {
        return label();
    }
}
