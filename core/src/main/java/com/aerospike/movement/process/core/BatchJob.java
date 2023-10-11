package com.aerospike.movement.process.core;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BatchJob {


    public static Config CONFIG = new Config();

    public static Map<String, Map<String, Object>> overridesFromConfig(final Configuration configuration) {
        throw ErrorUtil.unimplemented();
    }

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return new HashMap<>() {{

            }};
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }

        public static class Keys {
            public static final String BATCH_TASK_NAMES = "batch.task.names";
            public static final String BATCH_TASK_SETTINGS = "batch.task.settings";
        }
    }

    final List<Configuration> configs;
    final Map<String, Map<String, Object>> overrides = new HashMap<>();

    private BatchJob(List<Configuration> configs) {
        this.configs = configs;
    }

    public BatchJob withOverrides(Map<String, Map<String, Object>> overrides) {
        this.overrides.putAll(overrides);
        return this;
    }

    public static BatchJob of(Configuration... configs) {
        return new BatchJob(Arrays.stream(configs).collect(Collectors.toList()));
    }

    public void run() {
        if (!overrides.isEmpty()) {
            final Configuration config = configs.get(0);
            for (Map<String, Object> override : overrides.values()) {
                throw ErrorUtil.unimplemented();
//                CLI.run(ConfigurationUtil.configurationWithOverrides(config, override));
            }
        } else {
            for (Configuration config : configs) {
                throw ErrorUtil.unimplemented();

//                CLI.run(config);
            }
        }
    }
}
