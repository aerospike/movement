package com.aerospike.graph.move.emitter;

import java.util.Optional;
import java.util.stream.Stream;

public interface EmittedElement extends Emitable {
    Stream<String> propertyNames();

    Optional<Object> propertyValue(String name);

    String label();

    Stream<Emitable> stream();
}
