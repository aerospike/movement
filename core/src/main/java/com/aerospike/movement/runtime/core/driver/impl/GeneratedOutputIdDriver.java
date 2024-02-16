/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver.impl;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.runtime.core.driver.OutputId;
import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class GeneratedOutputIdDriver extends OutputIdDriver {

    private final long rangeTop;

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigUtil.getKeysFromClass(GeneratedOutputIdDriver.Config.Keys.class);
        }

        public static class Keys {
            public static final String RANGE_BOTTOM = "outputIdDriver.supplied.range.bottom";
            public static final String RANGE_TOP = "outputIdDriver.supplied.range.top";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.RANGE_TOP, String.valueOf(Long.MAX_VALUE));
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
    public Optional<OutputId> getNext(Optional<Emitable> passthru) {
        return getNext();
    }

    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static final AtomicReference<GeneratedOutputIdDriver> INSTANCE = new AtomicReference<>();


    private GeneratedOutputIdDriver(final Configuration config) {
        super(Config.INSTANCE, config);
        this.counter = new AtomicLong(Long.parseLong(Config.INSTANCE.getOrDefault(Config.Keys.RANGE_BOTTOM, config)));
        this.rangeTop = Long.parseLong(Config.INSTANCE.getOrDefault(Config.Keys.RANGE_TOP, config));
    }

    public static OutputIdDriver open(final Configuration config) {
        if (initialized.compareAndSet(false, true)) {
            INSTANCE.set(new GeneratedOutputIdDriver(config));
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
