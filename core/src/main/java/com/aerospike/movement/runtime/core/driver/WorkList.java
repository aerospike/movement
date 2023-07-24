package com.aerospike.movement.runtime.core.driver;

import org.apache.commons.configuration2.Configuration;

import java.util.*;

public class WorkList implements WorkChunk {

    private final Iterator<Object> iterator;
    private final List<Object> list;
    private final Configuration config;
    private final UUID id;

    private WorkList(final List<Object> list, final Configuration config) {
        this.config = config;
        this.id = UUID.randomUUID();
        this.list = list;
        this.iterator = list.iterator();
    }

    public static WorkList from(final Collection<Object> list, final Configuration config) {
        return new WorkList(new ArrayList<>(list), config);
    }

    @Override
    public WorkChunkId next() {
        return new WorkChunkId(iterator.next());
    }

    @Override
    public boolean hasNext() {
        if (!iterator.hasNext()) {
            this.onComplete(config);
            return false;
        }
        return true;
    }

    @Override
    public UUID getId() {
        return id;
    }
}
