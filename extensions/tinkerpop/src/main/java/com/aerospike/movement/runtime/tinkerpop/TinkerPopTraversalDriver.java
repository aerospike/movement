/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.runtime.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.local.Loadable;
import org.apache.commons.configuration2.Configuration;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class TinkerPopTraversalDriver extends WorkChunkDriver {
    protected TinkerPopTraversalDriver(ConfigurationBase configurationMeta, Configuration config) {
        super(configurationMeta, config);
    }

    @Override
    protected AtomicBoolean getInitialized() {
        return null;
    }

    @Override
    public void init(Configuration config) {

    }

    @Override
    public Optional<WorkChunk> getNext() {
        return Optional.empty();
    }
}
