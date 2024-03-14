/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.tinkerpop;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.structure.core.EmittedId;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Optional;
import java.util.stream.Stream;

import static com.aerospike.movement.emitter.core.Emitter.encodeToOutput;

public class TinkerPopEdge implements EmittedEdge {
    private final Edge edge;

    public TinkerPopEdge(final Edge edge) {
        this.edge = edge;
    }

    @Override
    public Stream<Emitable> emit(final Output output) {
        encodeToOutput(this,output);
        return Stream.empty();
    }

    @Override
    public EmittedId fromId() {
        return EmittedId.from(Long.valueOf(edge.outVertex().id().toString()));
    }

    @Override
    public EmittedId toId() {
        return EmittedId.from(Long.valueOf(edge.inVertex().id().toString()));
    }

    @Override
    public Stream<String> propertyNames() {
        return IteratorUtils.stream(edge.properties()).map(Property::key);
    }

    @Override
    public Optional<Object> propertyValue(final String name) {
        if (!edge.property(name).isPresent()) {
            return Optional.empty();
        }
        return Optional.of(edge.property(name).value());
    }

    @Override
    public String label() {
        return edge.label();
    }

    @Override
    public Stream<Emitable> stream() {
        return Stream.empty();
    }
}
