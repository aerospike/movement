package com.aerospike.graph.move.encoding.format.csv;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.structure.EmittedId;
import com.aerospike.graph.move.structure.EmittedIdImpl;

import java.util.Optional;
import java.util.stream.Stream;

public class CSVVertex implements EmittedVertex {

    private final CSVLine line;

    public CSVVertex(CSVLine line) {
        this.line = line;
    }

    @Override
    public Stream<String> propertyNames() {
        return line.propertyNames().stream();
    }

    @Override
    public Optional<Object> propertyValue(final String name) {
        final Object x = line.getEntry(name);
        return x.equals(CSVLine.CSVField.EMPTY) ? Optional.empty() : Optional.of(line.getEntry(name));
    }

    @Override
    public String label() {
        return (String) line.getEntry("~label");
    }

    @Override
    public Stream<Emitable> emit(final Output writer) {
        writer.vertexWriter(label()).writeVertex(this);
        return Stream.empty();
    }


    @Override
    public Stream<Emitable> stream() {
        return Stream.empty();
    }

    @Override
    public EmittedId id() {
        return new EmittedIdImpl(Long.valueOf((String) line.getEntry("~id")));
    }
}
