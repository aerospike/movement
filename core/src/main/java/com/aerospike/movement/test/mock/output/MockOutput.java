/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.test.mock.output;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.core.OutputWriter;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.emitter.MockEmitter;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MockOutput extends Loadable implements Output, OutputWriter {
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

    private final Encoder encoder;

    private MockOutput(final Configuration config) {
        super(Config.INSTANCE, config);
        this.encoder = RuntimeUtil.loadEncoder(config);
    }

    public static MockOutput open(final Configuration config) {
        return new MockOutput(config);
    }

    public static void clear() {

    }

    @Override
    public void init(final Configuration config) {

    }

    public static class Methods {
        public static final String WRITER = "writer";
        public static final String GET_METRICS = "getMetrics";
        public static final String WRITE_TO_OUTPUT = "writeToOutput";
        public static final String INIT = "init";
        public static final String FLUSH = "flush";
        public static final String CLOSE = "close";
        public static final String DROP_STORAGE = "dropStorage";
        public static final String READER = "reader";
    }

    @Override
    public OutputWriter writer(final Class type, final String label) {
        return (OutputWriter) MockUtil.onEvent(this.getClass(), Methods.WRITER, this, label).orElse(this);
    }

    @Override
    public Emitter reader(final Runtime.PHASE phase, final Class type, final Optional<String> label, final Configuration callerConfig) {
        final MockEmitter emitter = MockEmitter.open(callerConfig);
        return (Emitter) MockUtil.onEvent(this.getClass(), Methods.READER, emitter, label)
                .orElse(emitter);
    }


    @Override
    public Map<String, Object> getMetrics() {
        return (Map<String, Object>) MockUtil.onEvent(this.getClass(), Methods.GET_METRICS, this)
                .orElse(new HashMap<>() {{
                    put(Methods.WRITE_TO_OUTPUT, MockUtil.getHitCounter(MockOutput.class, Methods.WRITE_TO_OUTPUT));
                    put(Methods.FLUSH, MockUtil.getHitCounter(MockOutput.class, Methods.FLUSH));
                    put(Methods.CLOSE, MockUtil.getHitCounter(MockOutput.class, Methods.CLOSE));
                    put(Methods.DROP_STORAGE, MockUtil.getHitCounter(MockOutput.class, Methods.DROP_STORAGE));
                }});
    }

    @Override
    public void writeToOutput(final Optional<Emitable> item) {
        MockUtil.onEvent(this.getClass(), Methods.WRITE_TO_OUTPUT, this, item, item.map(it -> encoder.encode(it)));
    }


    @Override
    public void init() {
        MockUtil.onEvent(this.getClass(), Methods.INIT, this);
    }

    @Override
    public void flush() {
        MockUtil.onEvent(this.getClass(), Methods.FLUSH, this);
    }

    @Override
    public void onClose() {
        MockUtil.onEvent(this.getClass(), Methods.CLOSE, this);
    }

    @Override
    public void dropStorage() {
        MockUtil.onEvent(this.getClass(), Methods.DROP_STORAGE, this);
    }

    @Override
    public Optional<Encoder> getEncoder() {
        return Optional.ofNullable(encoder);
    }
}
