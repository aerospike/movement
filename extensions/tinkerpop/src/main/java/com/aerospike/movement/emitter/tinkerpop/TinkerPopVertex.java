/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.tinkerpop;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.structure.core.EmittedId;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Optional;
import java.util.stream.Stream;

public class TinkerPopVertex implements EmittedVertex {
    private final Vertex vertex;

    public TinkerPopVertex(final Vertex vertex) {
        this.vertex = vertex;
    }

    @Override
    public Stream<Emitable> emit(final Output output) {
        output.writer(EmittedVertex.class, vertex.label()).writeToOutput(this);
        return Stream.empty();
    }

    @Override
    public Stream<String> propertyNames() {
        return IteratorUtils.stream(vertex.properties()).map(p -> p.key());
    }

    @Override
    public Optional<Object> propertyValue(final String field) {
        if (field.equals("~id"))
            return Optional.of(vertex.id());
        if (field.equals("~label"))
            return Optional.of(vertex.label());
        //@todo multi properties
        if (!vertex.properties(field).hasNext() || !vertex.properties(field).next().isPresent()) {
            return Optional.empty();
        }
        return Optional.of(vertex.properties(field).next().value());
    }

    @Override
    public String label() {
        return vertex.label();
    }

    @Override
    public Stream<Emitable> stream() {
        return Stream.empty();
    }

    @Override
    public EmittedId id() {
        return EmittedId.from(Long.valueOf(vertex.id().toString()));
    }
}
