package com.aerospike.movement.performance;

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

/**
 * run read and write workloads
 * allow various stages to be run with different configurations in sequence
 * <p>
 * allow scaling number of threads, record errors and latencies
 * <p>
 * allow a plugin system to "trigger" external events, setupStage(stageName), endStage(stageName)
 */
public class PerformanceTask extends Task {

    static {
        RuntimeUtil.registerTaskAlias(PerformanceTask.class.getSimpleName(), PerformanceTask.class);
    }

    public static Task open(final Configuration config) {
        return new PerformanceTask(config);
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
            return ConfigurationUtil.getKeysFromClass(MockOutput.Config.Keys.class);
        }


        public static class Keys {

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

    protected PerformanceTask(final Configuration config) {
        super(Config.INSTANCE, config);
    }

    @Override
    public Map<String, Object> getMetrics() {
        return null;
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
        return List.of(Runtime.PHASE.ONE);
    }


}
