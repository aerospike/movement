package com.aerospike.graph.move.process.operations;

import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.process.Job;
import com.aerospike.graph.move.util.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Migrate extends Job {
    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String SOURCE_NAMESPACE = "migrate.sourceNamespace";
            public static final String DEST_NAMESPACE = "migrate.destNamespace";
            public static final String OLD_JAR = "migrate.oldJar";
            public static final String NEW_JAR = "migrate.newJar";

        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

    public Migrate(Configuration config) {
        super(config);
    }

    public static Map<String, Object> getBaseConfig() {
        return null;
    }
    public List<String> getRequiredArgs() {
        return List.of(Config.Keys.SOURCE_NAMESPACE, Config.Keys.DEST_NAMESPACE, Config.Keys.OLD_JAR, Config.Keys.NEW_JAR);
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
}
