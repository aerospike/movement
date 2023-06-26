package com.aerospike.graph.move.structure;

public class EmittedIdImpl implements EmittedId {
    public final Object id;

    public EmittedIdImpl(final Object id) {
        this.id = id;
    }

    @Override
    public Object getId() {
        return id;
    }
}
