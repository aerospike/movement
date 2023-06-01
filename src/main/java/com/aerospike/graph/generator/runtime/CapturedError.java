package com.aerospike.graph.generator.runtime;

import com.aerospike.graph.generator.structure.EmittedId;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class CapturedError {
    private final Exception error;
    private final EmittedId id;

    public CapturedError(Exception e, EmittedId id) {
        this.error = e;
        this.id = id;
        e.printStackTrace();
    }

    @Override
    public String toString() {
        return error.getMessage();
    }
}
