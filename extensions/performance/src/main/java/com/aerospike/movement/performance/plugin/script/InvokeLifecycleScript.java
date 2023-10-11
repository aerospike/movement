package com.aerospike.movement.performance.plugin.script;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.performance.plugin.LifecyclePlugin;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InvokeLifecycleScript implements LifecyclePlugin {
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
            public static final String START_SCRIPT_PREFIX = "performance.plugin.script.start.";
            public static final String END_SCRIPT_PREFIX = "performance.plugin.script.end.";
            public static final String SCRIPT_NAME_FORMAT = "stage-%s";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }

    public static final Config CONFIG = new Config();

    @Override
    public Optional<Throwable> setupStage(final String stageName, final Configuration config) {
        return Optional.empty();
    }

    @Override
    public Optional<Throwable> endStage(final String stageName, final Configuration config) {
        return Optional.empty();
    }
}
