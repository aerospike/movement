package com.aerospike.graph.move.emitter;

import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.util.ErrorUtil;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public interface EmittedElement extends Emitable {

    Stream<String> propertyNames();

    Optional<Object> propertyValue(String name);

    String label();

    Stream<Emitable> stream();
}
