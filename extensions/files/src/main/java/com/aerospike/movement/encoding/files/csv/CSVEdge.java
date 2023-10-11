package com.aerospike.movement.encoding.files.csv;


import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.graph.EmittedEdge;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.structure.core.EmittedId;
import com.aerospike.movement.structure.core.EmittedIdImpl;

import java.util.Optional;
import java.util.stream.Stream;

public class CSVEdge implements EmittedEdge {

    private final CSVLine line;

    public CSVEdge(CSVLine line) {
        this.line = line;
    }

    @Override
    public EmittedId fromId() {
        return new EmittedIdImpl(Long.valueOf((String) line.getEntry("~from")));
    }

    @Override
    public EmittedId toId() {
        return new EmittedIdImpl(Long.valueOf((String) line.getEntry("~to")));
    }

    @Override
    public Stream<String> propertyNames() {
        return line.propertyNames().stream();
    }

    @Override
    public Optional<Object> propertyValue(final String name) {
        return Optional.of(line.getEntry(name));
    }

    @Override
    public String label() {
        return line.getEntry("~label").toString();
    }

    @Override
    public Stream<Emitable> emit(final Output writer) {
        writer.writer(EmittedEdge.class,label()).writeToOutput(this);
        return Stream.empty();
    }

    @Override
    public Stream<Emitable> stream() {
        return Stream.empty();
    }
}
