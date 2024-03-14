/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.runtime.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.tinkerpop.TinkerPopGraphEmitter;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.driver.WorkList;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.iterator.Batched;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TinkerPopGraphDriver extends WorkChunkDriver {
    private Runtime.PHASE phase;


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
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};

    }

    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static Iterator<List<Object>> iterator;

    private TinkerPopGraphDriver(Runtime.PHASE phase, final Configuration config) {
        super(Config.INSTANCE, config);
        this.phase = phase;
    }

    @Override
    protected AtomicBoolean getInitialized() {
        return initialized;
    }

    public static TinkerPopGraphDriver open(final Configuration config) {
        return new TinkerPopGraphDriver(RuntimeUtil.getCurrentPhase(config), config);
    }

    @Override
    public void init(final Configuration config) {
        synchronized (TinkerPopGraphDriver.class) {
            if (!initialized.get()) {
                phase = RuntimeUtil.getCurrentPhase(config);
                final Class providerClass = RuntimeUtil.loadClass(TinkerPopGraphEmitter.CONFIG.getOrDefault(TinkerPopGraphEmitter.Config.Keys.GRAPH_PROVIDER, config));
                final GraphProvider graphProvider = (GraphProvider) RuntimeUtil.openClass(providerClass, config);
                final Graph graph = graphProvider.getProvided(GraphProvider.GraphProviderContext.INPUT);
                if (phase.equals(Runtime.PHASE.ONE)) {
//                    TinkerPopGraphDriver.iterator = Batched.batch(graph.vertices(), RuntimeUtil.getBatchSize(config));
                    TinkerPopGraphDriver.iterator = IteratorUtils.map(graph.vertices(), it -> List.of(it));

                } else if (phase.equals(Runtime.PHASE.TWO)) {
//                    TinkerPopGraphDriver.iterator = Batched.batch(graph.edges(), RuntimeUtil.getBatchSize(config));
                    TinkerPopGraphDriver.iterator = IteratorUtils.map(graph.edges(), it -> List.of(it));
                } else {
                    throw RuntimeUtil.getErrorHandler(this).handleFatalError(new RuntimeException("unknown phase"), phase);
                }
                initialized.set(true);
            }
        }
    }

    @Override
    public void onClose()  {
        synchronized (TinkerPopGraphDriver.class) {
            if (initialized.compareAndSet(true, false)) {
                iterator = null;
            }
        }
    }


    @Override
    public Optional<WorkChunk> getNext() {
        if (!initialized.get() || iterator == null)
            throw new IllegalStateException("TinkerPopGraphDriver not initialized");
        synchronized (iterator) {
            try {
                if (!iterator.hasNext()) return Optional.empty();
            } catch (IllegalStateException ise) {
                throw new RuntimeException(ise);
            }
            final WorkList list = WorkList.from(iterator.next());
            onNextValue(list);
            return Optional.of(list);
        }
    }

}
