package com.aerospike.graph.move.util;

import com.aerospike.graph.move.config.ConfigurationBase;
import org.apache.commons.configuration2.Configuration;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class Estimator {
    private final Configuration config;
    private final Method getMetrics;

    public Estimator(final Configuration config, final Class instrumentedClass) {
        this.config = config;
        this.getMetrics = RuntimeUtil.getMethod(instrumentedClass, Config.Keys.STATIC_METRICS_METHOD);
    }

    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String STATIC_METRICS_METHOD = "metrics";
            public static final String INSTRUMENTED_CLASS = "instrumented.class";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

}
