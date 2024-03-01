/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver.impl;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.runtime.core.driver.*;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

public class RangedWorkChunkDriver extends WorkChunkDriver {

    private final long rangeTop;
    private final int batchSize;


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
            return ConfigUtil.getKeysFromClass(RangedWorkChunkDriver.Config.Keys.class);
        }

        public static class Keys {
            public static final String RANGE_BOTTOM = "rangedWorkChunkDriver.supplied.range.bottom";
            public static final String RANGE_TOP = "rangedWorkChunkDriver.supplied.range.top";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.RANGE_BOTTOM, String.valueOf(0));
        }};
    }

    private final AtomicLong counter;


    public Optional<WorkChunk> getNext() {
        final long nextStart = counter.getAndAdd(batchSize);
        if (nextStart >= rangeTop) {
            return Optional.empty();
        }
        long end = Math.min(nextStart + batchSize, rangeTop);
        final List<Long> workIds = IteratorUtils.list(LongStream.range(nextStart,end).iterator());
        return Optional.of(WorkList.from(workIds));
    }


    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static final AtomicReference<RangedWorkChunkDriver> INSTANCE = new AtomicReference<>();


    private RangedWorkChunkDriver(final Configuration config) {
        super(Config.INSTANCE, config);
        this.counter = new AtomicLong(Long.parseLong(Config.INSTANCE.getOrDefault(Config.Keys.RANGE_BOTTOM, config)));
        this.rangeTop = Long.parseLong(Config.INSTANCE.getOrDefault(Config.Keys.RANGE_TOP, config));
        this.batchSize = RuntimeUtil.getBatchSize(config);

    }

    public static WorkChunkDriver open(final Configuration config) {
        if (initialized.compareAndSet(false, true)) {
            INSTANCE.set(new RangedWorkChunkDriver(config));
        }
        return INSTANCE.get();
    }

    @Override
    public void init(final Configuration config) {
        initialized.set(true);
    }


    @Override
    protected AtomicBoolean getInitialized() {
        return initialized;
    }

    @Override
    public void onClose()  {
        closeInstance();
    }

    public static void closeInstance() {
        initialized.set(false);
        INSTANCE.set(null);
    }
}
