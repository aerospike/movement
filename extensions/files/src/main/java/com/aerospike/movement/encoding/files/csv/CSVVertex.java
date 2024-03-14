/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.files.csv;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.structure.core.EmittedId;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;


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
        output.writer(EmittedVertex.class, label()).writeToOutput(Optional.of(this));
        return Stream.empty();
    }


    @Override
    public Stream<Emitable> stream() {
        return Stream.empty();
    }

    @Override
    public EmittedId id() {
        try{
            return EmittedId.from((String) line.getEntry("~id"));
        }catch (Exception e){
            RuntimeUtil.getLogger(CSVVertex.class.getSimpleName()).error(line.toString());
            throw new RuntimeException(e);
        }
    }
}
