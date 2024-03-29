/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.files.csv;


import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.structure.core.EmittedId;


import java.util.Optional;
import java.util.stream.Stream;

public class CSVEdge implements EmittedEdge {

    private final CSVLine line;

    public CSVEdge(CSVLine line) {
        this.line = line;
    }

    @Override
    public EmittedId fromId() {
        return EmittedId.from((String) line.getEntry("~from"));
    }

    @Override
    public EmittedId toId() {
        return EmittedId.from((String) line.getEntry("~to"));
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
    public Stream<Emitable> emit(final Output output) {
        output.writer(EmittedEdge.class, label()).writeToOutput(Optional.of(this));
        return Stream.empty();
    }

    @Override
    public Stream<Emitable> stream() {
        return Stream.empty();
    }
}
