/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.output.tinkerpop;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopTraversalEncoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.core.OutputWriter;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.error.ErrorUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TinkerPopTraversalOutput extends Loadable implements Output, OutputWriter {
    private final Encoder<Element> encoder;
    private final AtomicLong ioOperations;

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

    private TinkerPopTraversalOutput(final Encoder<Element> encoder, final Configuration config) {
        super(Config.INSTANCE, config);
        this.encoder = encoder;
        this.ioOperations = new AtomicLong(0);
    }


    public static TinkerPopTraversalOutput open(Configuration config) {
        return new TinkerPopTraversalOutput((Encoder<Element>) RuntimeUtil.lookupOrLoad(Encoder.class,config), config);
    }


    @Override
    public OutputWriter writer(final Class type, final String label) {
        return this;
    }

    @Override
    public Emitter reader(Runtime.PHASE phase, Class type, Optional<String> label, final Configuration callerConfig) {
        throw ErrorUtil.unimplemented();
    }


    @Override
    public Map<String, Object> getMetrics() {
        return new HashMap<>() {{
            put(RuntimeUtil.IO_OPS, ioOperations.get());
        }};
    }


    @Override
    public void writeToOutput(final Optional<Emitable> vertex) {
        vertex.flatMap(encoder::encode); //written during encoding
        ioOperations.addAndGet(1);
    }

    @Override
    public void init() {
    }

    @Override
    public void flush() {


    }

    @Override
    public void close() {
        ((TinkerPopTraversalEncoder) encoder).close();
    }

    @Override
    public void dropStorage() {
//        GraphTraversalSource g = ((TinkerPopTraversalEncoder) encoder).getTraversal();
//        g.V().drop().iterate();
    }

    @Override
    public Optional<Encoder> getEncoder() {
        return Optional.of(encoder);
    }

    @Override
    public String toString() {
        GraphTraversalSource g = ((TinkerPopTraversalEncoder) encoder).getTraversal();
        return g.toString();
    }

    @Override
    public void init(final Configuration config) {

    }
}
