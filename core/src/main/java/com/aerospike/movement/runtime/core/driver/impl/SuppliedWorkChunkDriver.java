package com.aerospike.movement.runtime.core.driver.impl;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.driver.WorkList;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.Batched;
import com.aerospike.movement.util.core.iterator.ConfiguredRangeSupplier;
import com.aerospike.movement.util.core.iterator.IteratorSupplier;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SuppliedWorkChunkDriver extends WorkChunkDriver {


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
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String ITERATOR_SUPPLIER = "workChunkDriver.supplied.iteratorSupplier";
            public static final String RANGE_BOTTOM = "workChunkDriver.supplied.range.bottom";
            public static final String RANGE_TOP = "workChunkDriver.supplied.range.top";
        }
        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
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

        final Optional<String> configuredSupplierClass = Optional.ofNullable(config.getString(Config.Keys.ITERATOR_SUPPLIER));
        final IteratorSupplier supplier;
        if (configuredSupplierClass.isPresent()) {
            Configuration rangeConfig = ConfigurationUtil.configurationWithOverrides(config, new MapConfiguration(new HashMap<>() {{
                put(ConfiguredRangeSupplier.Config.Keys.RANGE_BOTTOM, Config.INSTANCE.getOrDefault(Config.Keys.RANGE_BOTTOM, config));
                put(ConfiguredRangeSupplier.Config.Keys.RANGE_TOP, Config.INSTANCE.getOrDefault(Config.Keys.RANGE_TOP, config));
            }}));
            supplier = (IteratorSupplier) RuntimeUtil.openClassRef(configuredSupplierClass.get(), rangeConfig);
        } else if (Optional.ofNullable(suppliers.get(RuntimeUtil.getCurrentPhase(config))).isPresent()) {
            supplier = Optional.ofNullable(suppliers.get(RuntimeUtil.getCurrentPhase(config))).get();
        } else {
            throw new RuntimeException("No configured iterator supplier set for phase: " + RuntimeUtil.getCurrentPhase(config));
        }

        if (initialized.compareAndSet(false, true)) {
            final int batchSize = LocalParallelStreamRuntime.getBatchSize(config);

            SuppliedWorkChunkDriver.iterator = Batched.consume(supplier.get(), batchSize);
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
    public boolean hasNext() {
        synchronized (SuppliedWorkChunkDriver.class) {
            return initialized.get() && iterator.hasNext();
        }
    }

    @Override
    public WorkChunk next() {
        synchronized (SuppliedWorkChunkDriver.class) {
            onNext();
            final List<Object> x = iterator.next();
            final WorkList value = WorkList.from(x, config);
            onNextValue(value);
            return value;
        }
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
