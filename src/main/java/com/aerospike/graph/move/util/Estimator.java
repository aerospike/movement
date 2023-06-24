package com.aerospike.graph.move.util;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.commons.configuration2.Configuration;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Estimator {
    private final Configuration config;
    private final Method getMetrics;

    public Estimator(final Configuration config, final Class instrumentedClass) {
        this.config = config;
        this.getMetrics = RuntimeUtil.getMethod(instrumentedClass,Config.Keys.STATIC_METRICS_METHOD);
    }

    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String STATIC_METRICS_METHOD = "metrics";
            private static final String INSTRUMENTED_CLASS = "instrumented.class";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

    public static class InstrumentedUtil {
        public static long checkPointMetric(String name, Map<String, Long> metrics) {
            return metrics.get(name);
        }

        long deltaMetric(String name, long lastValue, Map<String, Long> metrics) {
            return metrics.get(name) - lastValue;
        }
    }

    public static class Instrumentation {
        public static Object open(Configuration config) {
            Class instrumentedClass = RuntimeUtil.loadClass(Config.Keys.INSTRUMENTED_CLASS);
            return InstrumentedClassWrapper.open(instrumentedClass, instrumentedClass);
        }

    }
    public static class InstrumentedClassWrapper {
        private static ConcurrentHashMap<String, AtomicLong> metrics = new ConcurrentHashMap<>();
        public static Object open(final Object originalInstance, Class<?> originalClass) {
            try {
                return new ByteBuddy()
                        .subclass(originalInstance.getClass())
                        .method(ElementMatchers.any())
                        .intercept(Advice.to(InstrumentedClassWrapper.class))
                        .make()
                        .load(originalClass.getClassLoader())
                        .getLoaded()
                        .getDeclaredConstructor()
                        .newInstance();
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        public static void incrementCounter(String methodName) {
            metrics.computeIfAbsent(methodName, k -> new AtomicLong(0)).incrementAndGet();
        }

    }
}
