package com.aerospike.movement.test.mock.task;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockTask extends Task {

    static {
        RuntimeUtil.registerTaskAlias(MockTask.class.getSimpleName(), MockTask.class);
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
            public static final String NAME = "name";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.NAME, "mock");
        }};
    }

    protected MockTask(final Configuration config) {
        super(Config.INSTANCE, config);
    }

    public static Task open(final Configuration config) {
        return new MockTask(config);
    }

    @Override
    public Map<String, Object> getMetrics() {
        return Map.of("mock",1);
    }

    @Override
    public boolean succeeded() {
        return true;
    }

    @Override
    public boolean failed() {
        return false;
    }

    @Override
    public List<Runtime.PHASE> getPhases() {
        return List.of(Runtime.PHASE.ONE);
    }

    @Override
    public void init(final Configuration config) {

    }

    @Override
    public void close() throws Exception {

    }
}