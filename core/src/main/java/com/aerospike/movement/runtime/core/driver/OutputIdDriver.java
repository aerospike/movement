/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.util.core.configuration.ConfigUtil;

import com.aerospike.movement.util.core.stream.sequence.PotentialSequence;
import org.apache.commons.configuration2.Configuration;

import java.util.*;

/**
 * IdSupplier provides chunks of available ids.
 * It is not necessary for a process to use every id provided by IdSupplier
 * getting a chunk is threadsafe, but an individual chunk should be consumed by the thread that it is asigned to
 */
public abstract class OutputIdDriver extends Loadable implements PotentialSequence<OutputId> {

    protected OutputIdDriver(ConfigurationBase base, Configuration config) {
        super(base, config);
    }

    public abstract Optional<OutputId> getNext();
    public abstract Optional<OutputId> getNext(Optional<Emitable> passthru);

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

    public static final Config CONFIG = new Config();


}
