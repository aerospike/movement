package com.aerospike.graph.move.util;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.commons.configuration2.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class JVMInstrumentation {
    public static Object open(Configuration config, final String target) {
        Class<?> instrumentedClass = RuntimeUtil.loadClass(target);
        return InstrumentedClassWrapper.open(instrumentedClass, instrumentedClass);
    }

    public static class InstrumentedUtil {
        public static long checkPointMetric(String name, Map<String, Long> metrics) {
            return metrics.get(name);
        }

        long deltaMetric(String name, long lastValue, Map<String, Long> metrics) {
            return metrics.get(name) - lastValue;
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
                throw ErrorUtil.unimplemented();
            }
        }

        public static void incrementCounter(String methodName) {
            metrics.computeIfAbsent(methodName, k -> new AtomicLong(0)).incrementAndGet();
        }

    }
}
