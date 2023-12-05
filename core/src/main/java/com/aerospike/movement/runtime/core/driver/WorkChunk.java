/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver;


import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Iterator;
import java.util.UUID;

/**
 * A WorkChunk should be consumed on a single thread
 * A WorkChunk represents a list of Work Items to be processed.
 * This may be files, ids from a source system, etc
 **/
public interface WorkChunk extends Iterator<WorkItem> {
    WorkItem next();

    boolean hasNext();

    UUID getId();

    default void onComplete(final Configuration config) {
        ((WorkChunkDriver) RuntimeUtil.lookupOrLoad(WorkChunkDriver.class, config)).acknowledgeComplete(this.getId());
    }

    default Iterator<WorkItem> iterator() {
        return this;
    }
}
