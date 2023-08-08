package com.aerospike.movement.runtime.core.driver;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.ConfigurationUtil;

import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE;

/**
 * IdSupplier provides chunks of available ids.
 * It is not necessary for a process to use every id provided by IdSupplier
 * getting a chunk is threadsafe, but an individual chunk should be consumed by the thread that it is asigned to
 */
public abstract class OutputIdDriver extends Loadable implements OptionalSequence<OutputId> {

    protected OutputIdDriver(ConfigurationBase base, Configuration config) {
        super(base, config);
    }

    public abstract Optional<OutputId> getNext();

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
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }

    public static final Config CONFIG = new Config();


}
