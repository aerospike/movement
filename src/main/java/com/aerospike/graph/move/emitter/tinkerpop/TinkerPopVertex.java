package com.aerospike.graph.move.emitter.tinkerpop;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.structure.EmittedId;
import com.aerospike.graph.move.structure.EmittedIdImpl;
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
    public Stream<Emitable> emit(final Output writer) {
        writer.vertexWriter(vertex.label()).writeVertex(this);
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
        return new EmittedIdImpl(Long.valueOf(vertex.id().toString()));
    }
}
