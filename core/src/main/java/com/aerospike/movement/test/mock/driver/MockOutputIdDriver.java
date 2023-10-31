/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.test.mock.driver;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.runtime.core.driver.OutputId;
import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class MockOutputIdDriver extends OutputIdDriver {
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
            return ConfigurationUtil.getKeysFromClass(MockOutput.Config.Keys.class);
        }


        public static class Keys {

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

    protected MockOutputIdDriver(final Configuration config) {
        super(Config.INSTANCE, config);
    }

    public static class Methods {
        public static final String INIT = "init";
        public static final String GET_NEXT = "getNext";
        public static final String CLOSE = "close";
    }

    public static MockOutputIdDriver open(Configuration config) {
        return new MockOutputIdDriver(config);
    }

    @Override
    public void init(final Configuration config) {
        MockUtil.onEvent(this.getClass(), Methods.INIT, this);
    }

    @Override
    public Optional<OutputId> getNext() {
        return Optional.of(OutputId.create(MockUtil.onEvent(this.getClass(), Methods.GET_NEXT, this).get()));
    }

    @Override
    public Optional<OutputId> getNext(Optional<Emitable> passthru) {
        return getNext();
    }

    @Override
    public void close() throws Exception {
        MockUtil.onEvent(this.getClass(), Methods.CLOSE, this);
    }
}
