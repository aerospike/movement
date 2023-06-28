package com.aerospike.graph.move.emitter.tinkerpop;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.structure.EmittedId;
import com.aerospike.graph.move.structure.EmittedIdImpl;
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
        writer.edgeWriter(edge.label()).writeEdge(this);
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
