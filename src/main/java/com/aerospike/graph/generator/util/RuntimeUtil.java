package com.aerospike.graph.generator.util;

import com.aerospike.graph.generator.emitter.Emitable;
import com.aerospike.graph.generator.emitter.Emitter;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.output.Output;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Stream;

public class RuntimeUtil {
    private static final Logger logger = LoggerFactory.getLogger(RuntimeUtil.class);
    public static final String STATIC_OPEN_METHOD = "open";

    public static Output loadOutput(final Configuration config) {
        logger.debug("Loading output");
        return (Output) openClassRef(config.getString(ConfigurationBase.Keys.OUTPUT), config);
    }

    public static Encoder loadEncoder(final Configuration config) {
        logger.debug("Loading encoder");
        return (Encoder) openClassRef(config.getString(ConfigurationBase.Keys.ENCODER), config);
    }

    public static Emitter loadEmitter(final Configuration config) {
        logger.debug("Loading emitter");
        return (Emitter) openClassRef(config.getString(ConfigurationBase.Keys.EMITTER), config);
    }

    public static Object openClassRef(final String className, final Configuration config) {
        try {
            final Class clazz = Class.forName(className);
            return clazz.getMethod(STATIC_OPEN_METHOD, Configuration.class).invoke(null, config);
        } catch (Exception e) {
            throw new RuntimeException("Error loading class: " + className, e);
        }
    }

    public static void invokeClassMain(final String className, Object[] args) {
        final Class clazz = loadClass(className);
        try {
            clazz.getMethod("main", String[].class).invoke(null, new Object[]{args});
        } catch (Exception e) {
            throw new RuntimeException("Error loading class: " + className, e);
        }
    }

    public static Class loadClass(final String className) {
        try {
            return Class.forName(className);
        } catch (Exception e) {
            throw new RuntimeException("Error loading class: " + className, e);
        }
    }

    public static Configuration loadConfiguration(String propertiesFile) {
        try {
            Properties catalogProps = new Properties();
            catalogProps.load(new FileInputStream(propertiesFile));
            return new MapConfiguration(catalogProps);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Iterator<Emitable> walk(Stream<Emitable> input, Output writer) {
        return IteratorUtils.flatMap(input.iterator(), emitable ->
                IteratorUtils.flatMap(emitable.emit(writer).iterator(),
                        emitable1 -> {
                            try {
                                return walk(emitable1.emit(writer), writer);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
        );
    }

    public static Method getMethod(final Class instrumentedClass, final String method) {
        try {
            return instrumentedClass.getMethod(method);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}

