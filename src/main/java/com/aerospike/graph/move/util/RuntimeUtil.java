package com.aerospike.graph.move.util;

import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.encoding.Decoder;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.output.Output;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

public class RuntimeUtil {
    private static final Logger logger = LoggerFactory.getLogger(RuntimeUtil.class);
    public static final String STATIC_OPEN_METHOD = "open";
    final static List<Output> outputs = Collections.synchronizedList(new ArrayList<>());
    final static List<Emitter> emitters = Collections.synchronizedList(new ArrayList<>());
    final static List<Encoder> encoders = Collections.synchronizedList(new ArrayList<>());

    public static Output loadOutput(final Configuration config) {
        logger.debug("Loading output");
        Output x = (Output) openClassRef(config.getString(ConfigurationBase.Keys.OUTPUT), config);
        outputs.add(x);
        return x;
    }

    public static Encoder loadEncoder(final Configuration config) {
        logger.debug("Loading encoder");
        Encoder x = (Encoder) openClassRef(config.getString(ConfigurationBase.Keys.ENCODER), config);
        encoders.add(x);
        return x;
    }

    public static Emitter loadEmitter(final Configuration config) {
        logger.debug("Loading emitter");
        Emitter x = (Emitter) openClassRef(config.getString(ConfigurationBase.Keys.EMITTER), config);
        emitters.add(x);
        return x;
    }


    public static Object openClassRef(final String className, final Configuration config) {
        try {
            final Class clazz = Class.forName(className);
            return clazz.getMethod(STATIC_OPEN_METHOD, Configuration.class).invoke(null, config);
        } catch (Exception e) {
            throw new RuntimeException("Error loading class: " + className, e);
        }
    }

    public static void invokeClassMain(final String className, final Object[] args) {
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

    public static Configuration loadConfiguration(final String propertiesFile) {
        try {
            Properties catalogProps = new Properties();
            catalogProps.load(new FileInputStream(propertiesFile));
            return new MapConfiguration(catalogProps);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Iterator<Emitable> walk(final Stream<Emitable> input, final Output writer) {
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


    public static Decoder<String> loadDecoder(final Configuration config) {
        logger.debug("Loading decoder");
        return (Decoder<String>) openClassRef(config.getString(ConfigurationBase.Keys.DECODER), config);
    }
    private static Object nextOrLoad(final Iterator<?> iterator, Callable<Object> loader){
        if(iterator.hasNext())
            return iterator.next();
        try {
            return loader.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static Object lookup(final Class targetClass, final Configuration config) {
        if (Output.class.isAssignableFrom(targetClass))
            return nextOrLoad(outputs.iterator(), () -> loadOutput(config));
        if (Emitter.class.isAssignableFrom(targetClass))
            return nextOrLoad(emitters.iterator(), () -> loadEmitter(config));
        if (Encoder.class.isAssignableFrom(targetClass))
            return nextOrLoad(encoders.iterator(), () -> loadEncoder(config));
        else
            throw new RuntimeException("Cannot find class: " + targetClass.getName());
    }
}

