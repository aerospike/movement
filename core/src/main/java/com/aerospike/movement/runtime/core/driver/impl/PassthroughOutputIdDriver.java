/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver.impl;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.runtime.core.driver.OutputId;
import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.structure.core.EmittedId;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PassthroughOutputIdDriver extends OutputIdDriver {

    private final long rangeTop;

    public static class Config extends ConfigurationBase {
        public static final PassthroughOutputIdDriver.Config INSTANCE = new PassthroughOutputIdDriver.Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(PassthroughOutputIdDriver.Config.Keys.class);
        }

        public static class Keys {
            public static final String RANGE_BOTTOM = "outputIdDriver.supplied.range.bottom";
            public static final String RANGE_TOP = "outputIdDriver.supplied.range.top";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(PassthroughOutputIdDriver.Config.Keys.RANGE_TOP, String.valueOf(Long.MAX_VALUE));
        }};
    }

    private final AtomicLong counter;


    @Override
    public Optional<OutputId> getNext() {
        final long next = counter.getAndIncrement();
        if (next >= rangeTop) {
            return Optional.empty();
        }
        return Optional.of(OutputId.create(next));
    }

    @Override
    public Optional<OutputId> getNext(final Optional<Emitable> passthru) {
        if (passthru.isEmpty())
            return Optional.empty();
        final Emitable emitable = passthru.get();
        final EmittedId id;
        if (EmittedVertex.class.isAssignableFrom(emitable.getClass()))
            id = ((EmittedVertex) emitable).id();
        else if (EmittedEdge.class.isAssignableFrom(emitable.getClass()))
            id = ((EmittedEdge) emitable).toId();
        else
            throw RuntimeUtil.getErrorHandler(this).handleError(new RuntimeException("unknown passthrough emitable type: " + emitable.getClass()), emitable);
        return Optional.of(OutputId.create(id.getId()));
    }

    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static final AtomicReference<PassthroughOutputIdDriver> INSTANCE = new AtomicReference<>();


    private PassthroughOutputIdDriver(final Configuration config) {
        super(PassthroughOutputIdDriver.Config.INSTANCE, config);
        this.counter = new AtomicLong(Long.parseLong(PassthroughOutputIdDriver.Config.INSTANCE.getOrDefault(PassthroughOutputIdDriver.Config.Keys.RANGE_BOTTOM, config)));
        this.rangeTop = Long.parseLong(PassthroughOutputIdDriver.Config.INSTANCE.getOrDefault(PassthroughOutputIdDriver.Config.Keys.RANGE_TOP, config));
    }

    public static OutputIdDriver open(final Configuration config) {
        if (initialized.compareAndSet(false, true)) {
            INSTANCE.set(new PassthroughOutputIdDriver(config));
        }
        return INSTANCE.get();
    }

    @Override
    public void init(final Configuration config) {

    }


    @Override
    public void close() throws Exception {
        closeInstance();
    }

    public static void closeInstance() {
        initialized.set(false);
        INSTANCE.set(null);
    }
}