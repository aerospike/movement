/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.stream.sequence.PotentialSequence;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/*
A work chunk driver should return groups of work (Work Chunks) from the source on the left hand side.
These chunks may either be groups of Ids that indicate what to read the source, or they may wrap the actual
input elements (pass through)
Pass through is more efficient (avoids double read), but some cases may require ids (distributed mode)
 */
public abstract class WorkChunkDriver extends Loadable implements PotentialSequence<WorkChunk> {
    private static final AtomicReference<WorkChunkDriver> INSTANCE = new AtomicReference<>();
    protected final Configuration config;
    private final AtomicLong chunksAcknowledged;

    protected final ErrorHandler errorHandler;
    public static final AtomicLong metric = new AtomicLong(0);

    protected WorkChunkDriver(final ConfigurationBase configurationMeta, final Configuration config) {
        super(configurationMeta, config);
        this.config = config;
        this.chunksAcknowledged = new AtomicLong(0);
        this.errorHandler = RuntimeUtil.getErrorHandler(this, config);
    }



    protected abstract AtomicBoolean getInitialized();

    protected WorkChunk onNextValue(final WorkChunk value) {
        metric.incrementAndGet();
        return value;
    }

    public void onClose()  {
        chunksAcknowledged.set(0);
        metric.set(0);
        getInitialized().set(false);
        INSTANCE.set(null);
    }


    public static WorkChunkDriver implicit(final Emitter emitter, final Configuration config) {
        if (!Emitter.SelfDriving.class.isAssignableFrom(emitter.getClass()))
            throw new RuntimeException("emitter must implement SelfDriving to get an implicit WorkChunkDriver");
        else return ((Emitter.SelfDriving) emitter).driver(config);
    }
}
