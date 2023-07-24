package com.aerospike.movement.emitter.core.graph;

import com.aerospike.movement.emitter.core.Emitable;

import java.util.Optional;
import java.util.stream.Stream;


public interface EmitableGraphElement extends Emitable {

    Stream<String> propertyNames();

    Optional<Object> propertyValue(String name);

    String label();

    Stream<Emitable> stream();
}
