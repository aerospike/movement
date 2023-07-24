package com.aerospike.movement.encoding.files.csv;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.graph.EmittedVertex;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.structure.core.EmittedId;
import com.aerospike.movement.structure.core.EmittedIdImpl;

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
    public Stream<Emitable> emit(final Output output) {
        output.writer(EmittedVertex.class, label()).writeToOutput(this);
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
