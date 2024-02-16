/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver.impl;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.driver.WorkList;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.Batched;
import com.aerospike.movement.util.core.iterator.IteratorSupplier;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SuppliedWorkChunkDriver extends WorkChunkDriver {
    @Override
    public Optional<WorkChunk> getNext() {
        synchronized (SuppliedWorkChunkDriver.class) {
            if (!initialized.get()) {
                throw new IllegalStateException("WorkChunkDriver not initialized");
            }
            if (!iterator.hasNext()) {
                return Optional.empty();
            }
            RuntimeUtil.blockOnBackpressure();
            final List<Object> x = iterator.next();
            final WorkList value = WorkList.from(x, config);
            onNextValue(value);
            return Optional.of(value);
        }
    }

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
            return ConfigUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String ITERATOR_SUPPLIER_PHASE_ONE = "workChunkDriver.supplied.iteratorSupplier.phase.one";
            public static final String ITERATOR_SUPPLIER_PHASE_TWO = "workChunkDriver.supplied.iteratorSupplier.phase.two";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
//            put(Keys.RANGE_BOTTOM, String.valueOf(0L));
        }};

    }


    private static final ConcurrentHashMap<Runtime.PHASE, IteratorSupplier> suppliers = new ConcurrentHashMap<>();
    private static final AtomicReference<SuppliedWorkChunkDriver> INSTANCE = new AtomicReference<>();
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static Iterator<List<Object>> iterator;

    protected SuppliedWorkChunkDriver(final Configuration config) {
        super(Config.INSTANCE, config);
    }

    public static void setIteratorSupplierForPhase(final Runtime.PHASE phase, final IteratorSupplier supplier) {
        SuppliedWorkChunkDriver.suppliers.put(phase, supplier);
    }

    public static void clearSupplierForPhase(final Runtime.PHASE phase) {
        SuppliedWorkChunkDriver.suppliers.remove(phase);
    }

    public static SuppliedWorkChunkDriver open(final Configuration config) {
        final Optional<String> configuredSupplierClass = RuntimeUtil.getCurrentPhase(config)
                .equals(Runtime.PHASE.ONE) ?
                Optional.ofNullable(config.getString(Config.Keys.ITERATOR_SUPPLIER_PHASE_ONE)) :
                Optional.ofNullable(config.getString(Config.Keys.ITERATOR_SUPPLIER_PHASE_TWO));


        final IteratorSupplier supplier;
        if (configuredSupplierClass.isPresent()) {
            supplier = (IteratorSupplier) RuntimeUtil.openClassRef(configuredSupplierClass.get(), config);
        } else if (Optional.ofNullable(suppliers.get(RuntimeUtil.getCurrentPhase(config))).isPresent()) {
            supplier = Optional.ofNullable(suppliers.get(RuntimeUtil.getCurrentPhase(config))).get();
        } else {
            throw RuntimeUtil.getErrorHandler(SuppliedWorkChunkDriver.class, config).handleError(new RuntimeException("No configured iterator supplier set for phase: " + RuntimeUtil.getCurrentPhase(config)));
        }

        if (initialized.compareAndSet(false, true)) {
            final int batchSize = RuntimeUtil.getBatchSize(config);

            SuppliedWorkChunkDriver.iterator = Batched.batch(supplier.get(), batchSize);
            INSTANCE.set(new SuppliedWorkChunkDriver(config));
        }
        return INSTANCE.get();
    }

    @Override
    public void init(final Configuration config) {
        initialized.compareAndSet(false, true);
    }

    @Override
    public void acknowledgeComplete(final UUID workChunkId) {
        super.acknowledgeComplete(workChunkId);
    }

    @Override
    protected AtomicBoolean getInitialized() {
        return initialized;
    }


    @Override
    public void close() throws Exception {
        closeStatic();
        super.close();
    }

    public static void closeStatic() throws Exception {
        initialized.set(false);
        INSTANCE.set(null);
        if (iterator instanceof AutoCloseable) {
            ((AutoCloseable) iterator).close();
        }
        iterator = null;
    }
}
