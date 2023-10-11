package com.aerospike.movement.emitter.tinkerpop;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.graph.EmittedEdge;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.structure.core.EmittedId;
import com.aerospike.movement.structure.core.EmittedIdImpl;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Optional;
import java.util.stream.Stream;

public class TinkerPopEdge implements EmittedEdge {
    private final Edge edge;

    public TinkerPopEdge(final Edge edge) {
        this.edge = edge;
    }

    @Override
    public Stream<Emitable> emit(final Output writer) {
        writer.writer(EmittedEdge.class, edge.label()).writeToOutput(this);
        return Stream.empty();
    }

    @Override
    public EmittedId fromId() {
        return new EmittedIdImpl(Long.valueOf(edge.outVertex().id().toString()));
    }

    @Override
    public EmittedId toId() {
        return new EmittedIdImpl(Long.valueOf(edge.inVertex().id().toString()));
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
