/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.output.tinkerpop;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.emitter.tinkerpop.TinkerPopGraphEmitter;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopGraphEncoder;
import com.aerospike.movement.runtime.tinkerpop.TinkerPopGraphDriver;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopTraversalEncoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.core.OutputWriter;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopGraphDecoder;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TinkerPopGraphOutput extends Loadable implements Output, OutputWriter {
    private final Encoder<Element> encoder;
    private final AtomicLong vertexMetric;
    private final AtomicLong edgeMetric;

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
            return ConfigUtil.getKeysFromClass(TinkerPopTraversalEncoder.Config.Keys.class);
        }


        public static class Keys {
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }


    public TinkerPopGraphOutput(final TinkerPopGraphEncoder encoder, final Configuration config) {
        super(Config.INSTANCE, config);
        this.encoder = encoder;
        this.vertexMetric = new AtomicLong(0);
        this.edgeMetric = new AtomicLong(0);
    }

    public static TinkerPopGraphOutput open(Configuration config) {
        return new TinkerPopGraphOutput((TinkerPopGraphEncoder) RuntimeUtil.lookupOrLoad(Encoder.class,config), config);
    }

    public static Configuration getOutputConfig() {
        return new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.OUTPUT, TinkerPopGraphOutput.class.getName());
            put(ConfigurationBase.Keys.ENCODER, TinkerPopGraphDecoder.class.getName());
        }});
    }


    @Override
    public OutputWriter writer(Class type, String label) {
        return this;
    }

    @Override
    public Emitter reader(Runtime.PHASE phase, Class type, Optional<String> label, final Configuration callerConfig) {
        final Configuration readerConfig = ConfigUtil.withOverrides(this.config,
                new HashMap<>() {{
                    put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, TinkerPopGraphDriver.class.getName());
                    put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_TWO, TinkerPopGraphDriver.class.getName());
                    put(ConfigurationBase.Keys.EMITTER,TinkerPopGraphEmitter.class.getName());
                    put(ConfigurationBase.Keys.INTERNAL_PHASE_INDICATOR, phase.name());
                }});
        return TinkerPopGraphEmitter.open(readerConfig);
    }

    @Override
    public Map<String, Object> getMetrics() {
        return new HashMap<>() {{
            put("vertices", vertexMetric.get());
            put("edges", edgeMetric.get());
        }};
    }


    @Override
    public void writeToOutput(final Optional<Emitable> item) {
        item.map(emitable -> {
            incrementMetrics(emitable);
            encoder.encode(emitable);
            return null;
        });
    }

    private void incrementMetrics(final Emitable item) {
        if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            vertexMetric.addAndGet(1);
        }
        if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            edgeMetric.addAndGet(1);
        }
    }

    @Override
    public void init() {
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        try {
            encoder.close();
        } catch (Exception e) {
            throw errorHandler.handleError(e);
        }
    }

    @Override
    public void dropStorage() {
        ((TinkerPopGraphEncoder) encoder).getGraph().traversal().V().drop().iterate();
    }

    @Override
    public Optional<Encoder> getEncoder() {
        return Optional.of(encoder);
    }

    @Override
    public void init(final Configuration config) {

    }
}
