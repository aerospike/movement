package com.aerospike.graph.move.encoding.format.csv;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.structure.EmittedId;
import com.aerospike.graph.move.structure.EmittedIdImpl;

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
        writer.edgeWriter(label()).writeEdge(this);
        return Stream.empty();
    }

    @Override
    public Stream<Emitable> stream() {
        return Stream.empty();
    }
}
