package com.aerospike.movement.runtime.core.driver;

import com.aerospike.movement.structure.core.EmittedId;

public class WorkChunkId implements EmittedId {
    protected final Object id;

    public WorkChunkId(final Object id) {
        this.id = id;
    }

    @Override
    public Object getId() {
        return id;
    }
}
