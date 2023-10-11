package com.aerospike.movement.process.tasks.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Migrate extends Task {

    static {
        RuntimeUtil.registerTaskAlias(Migrate.class.getSimpleName(), Migrate.class);
    }

    @Override
    public void init(final Configuration config) {

    }

    @Override
    public void close() throws Exception {

    }

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String,Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }

        public static class Keys {
            public static final String SOURCE_NAMESPACE = "migrate.sourceNamespace";
            public static final String DEST_NAMESPACE = "migrate.destNamespace";
            public static final String OLD_JAR = "migrate.oldJar";
            public static final String NEW_JAR = "migrate.newJar";
            public static final String OLD_VERSION = "migrate.batchSize";
            public static final String NEW_VERSION = "migrate.batchDelay";

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }


    public static Task open(final Configuration callConfig) {
        return new Migrate(callConfig);
    }

    public Migrate(final Configuration config) {
        super(Config.INSTANCE, config);
    }


    @Override
    public Configuration getConfig(Configuration config) {
        return config;
    }

    @Override
    public Map<String, Object> getMetrics() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public boolean succeeded() {
        return false;
    }

    @Override
    public boolean failed() {
        return false;
    }

    @Override
    public List<Runtime.PHASE> getPhases() {
        return List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO);
    }
}
