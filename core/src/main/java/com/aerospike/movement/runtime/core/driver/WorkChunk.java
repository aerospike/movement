package com.aerospike.movement.runtime.core.driver;


import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Iterator;
import java.util.UUID;

/**
 * A WorkUnit should be consumed on a single thread
 * A WorkUnit represents a list of Input ids to be processed.
 * This may be files, ids from a source system, etc
 * <p>
 * When complete, a WorkUnit must notify the WorkUnitSupplier it has been completed
 */
public interface WorkChunk extends Iterator<WorkChunkId> {
    WorkChunkId next();

    boolean hasNext();

    UUID getId();

    default void onComplete(final Configuration config) {
        ((WorkChunkDriver) RuntimeUtil.lookupOrLoad(WorkChunkDriver.class, config)).acknowledgeComplete(this.getId());
    }

    default Iterator<WorkChunkId> iterator(){
        return this;
    }
}
