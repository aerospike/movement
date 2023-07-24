package com.aerospike.movement.util.core;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.encoding.core.Decoder;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.logging.core.Logger;
import com.aerospike.movement.logging.core.impl.SystemLogger;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.reflections.scanners.Scanners.SubTypes;

public class RuntimeUtil {
    public static ErrorHandler getErrorHandler(final Object loggedObject, final Configuration config) {
        return loadErrorHandler(loggedObject, config);
    }

    public static Logger getLogger(final Object context) {
        return new SystemLogger(context);
    }

    public static void halt() {
        LocalParallelStreamRuntime.halt();
    }

    public enum DelayType {
        IO_THREAD_INIT
    }

    public static int getAvailableProcessors() {
        return java.lang.Runtime.getRuntime().availableProcessors();
    }

    public static final String STATIC_OPEN_METHOD = "open";


    public static Object openClassRef(final String className, final Configuration config) {
        try {
            final Class clazz = Optional.ofNullable(Class.forName(className)).orElseThrow(() ->
                    new RuntimeException("Could not load class: " + className));
            return clazz.getMethod(STATIC_OPEN_METHOD, Configuration.class).invoke(null, config);
        } catch (NoSuchMethodException nsm) {
            throw new RuntimeException(String.format("Class %s does not properly implement a static open(Configuration) method", className));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error opening class %s", className), e);
        }
    }

    public static Method getMethod(final Class clazz, final String method) {
        try {
            return clazz.getMethod(method);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static void invokeClassMain(final String className, final Object[] args) {
        invokeMethod(className, "main", null, args);
    }

    public static Optional<Object> invokeMethod(final String className, final String methodName, final Object object, final Object[] args) {
        final Class clazz = loadClass(className);
        return invokeMethod(clazz, methodName, object, args);
    }

    public static Optional<Object> invokeMethod(final Class clazz, final String methodName, final Object object, final Object[] args) {
        try {
            return Optional.ofNullable(clazz.getMethod(methodName, String[].class).invoke(object, new Object[]{args}));
        } catch (Exception e) {
            throw new RuntimeException("Error invoking method: " + methodName, e);
        }
    }

    public static Object getStaticFieldValue(final Class clazz, final String fieldName) {
        try {
            return clazz.getField(fieldName).get(null);
        } catch (Exception e) {
            throw new RuntimeException("Error getting field: " + fieldName, e);
        }
    }

    public static Class loadClass(final String className) {
        try {
            return Class.forName(className);
        } catch (Exception e) {
            throw new RuntimeException("Error loading class: " + className, e);
        }
    }

    public static Class loadInnerClass(final String outerClassName, final String innerClassName) {
        final String name = (String.format("%s$%s", outerClassName, innerClassName));
        try {
            return Class.forName(name);
        } catch (Exception e) {
            throw new RuntimeException("Error loading class: " + name, e);
        }
    }

    public static Output loadOutput(final Configuration config) {
        Output x = (Output) load(ConfigurationBase.Keys.OUTPUT, config);
        LocalParallelStreamRuntime.outputs.add(x);
        return x;
    }

    public static Encoder loadEncoder(final Configuration config) {
        Encoder x = (Encoder) load(ConfigurationBase.Keys.ENCODER, config);
        LocalParallelStreamRuntime.encoders.add(x);
        return x;
    }

    public static Emitter loadEmitter(final Configuration config) {
        Emitter x = (Emitter) openClassRef(config.getString(ConfigurationBase.Keys.EMITTER), config);
        LocalParallelStreamRuntime.emitters.add(x);
        return x;
    }

    public static OutputIdDriver loadOutputIdDriver(final Configuration config) {
        final OutputIdDriver it = (OutputIdDriver) load(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, config);
        LocalParallelStreamRuntime.outputIdDriver.set(it);
        return it;
    }


    public static WorkChunkDriver loadWorkChunkDriver(final Configuration config) {
        final WorkChunkDriver it = (WorkChunkDriver) load(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, config);
        LocalParallelStreamRuntime.workChunkDriver.set(it);
        return it;
    }

    public static Decoder<String> loadDecoder(final Configuration config) {
        final Decoder<String> it = (Decoder<String>) load(ConfigurationBase.Keys.DECODER, config);
        LocalParallelStreamRuntime.decoders.add(it);
        return it;
    }

    public static Object load(final String configName, final Configuration config) {
        final String driverName = Optional.ofNullable(config.getString(configName)).orElseThrow(() -> new RuntimeException("No work chunk driver specified"));
        return openClassRef(driverName, config);
    }

    private static Object nextOrLoad(final Iterator<Object> existing, Callable<Object> loader, Optional<UUID> optionalId) {
        final Iterator<Object> iterator = optionalId
                .map(uuid -> IteratorUtils.filter(existing, lodable -> ((Loadable) lodable).getId().equals(uuid)))
                .orElse(existing);
        if (iterator.hasNext()) {
            return iterator.next();
        }
        try {
            return loader.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object lookupOrLoad(final Class targetClass, final Configuration config) {
        return lookupOrLoad(targetClass, config, Optional.empty());
    }

    public static Iterator<Object> lookup(final Class targetClass) {
        if (Output.class.isAssignableFrom(targetClass))
            return IteratorUtils.wrap(LocalParallelStreamRuntime.outputs);
        if (Emitter.class.isAssignableFrom(targetClass))
            return IteratorUtils.wrap(LocalParallelStreamRuntime.emitters);
        if (Encoder.class.isAssignableFrom(targetClass))
            return IteratorUtils.wrap(LocalParallelStreamRuntime.encoders);
        if (Decoder.class.isAssignableFrom(targetClass))
            return IteratorUtils.wrap(LocalParallelStreamRuntime.decoders);
        if (OutputIdDriver.class.isAssignableFrom(targetClass))
            return (Iterator<Object>) IteratorUtils.wrap(Optional.ofNullable(LocalParallelStreamRuntime.outputIdDriver.get()).stream().iterator());
        if (WorkChunkDriver.class.isAssignableFrom(targetClass))
            return (Iterator<Object>) IteratorUtils.wrap(Optional.ofNullable(LocalParallelStreamRuntime.workChunkDriver.get()).stream().iterator());
        else
            return Collections.emptyIterator();
    }

    public static Object lookupOrLoad(final Class targetClass, final Configuration config, Optional<UUID> loadableId) {
        if (Output.class.isAssignableFrom(targetClass))
            return nextOrLoad(lookup(targetClass), () -> loadOutput(config), loadableId);
        if (Emitter.class.isAssignableFrom(targetClass))
            return nextOrLoad(lookup(targetClass), () -> loadEmitter(config), loadableId);
        if (Encoder.class.isAssignableFrom(targetClass))
            return nextOrLoad(lookup(targetClass), () -> loadEncoder(config), loadableId);
        if (Decoder.class.isAssignableFrom(targetClass))
            return nextOrLoad(lookup(targetClass), () -> loadDecoder(config), loadableId);
        if (OutputIdDriver.class.isAssignableFrom(targetClass))
            return nextOrLoad(lookup(targetClass), () -> loadOutputIdDriver(config), loadableId);
        if (WorkChunkDriver.class.isAssignableFrom(targetClass))
            return nextOrLoad(lookup(targetClass), () -> loadWorkChunkDriver(config), loadableId);
        if (Task.class.isAssignableFrom(targetClass))
            return nextOrLoad(lookup(targetClass), () -> loadTask(targetClass.getName(), config), loadableId);
        else
            throw new RuntimeException("Cannot find class: " + targetClass.getName());
    }

    private static Object loadTask(final String name, final Configuration config) {
        return openClassRef(name, config);
    }


    public static Optional<Throwable> closeWrap(final AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            return Optional.of(e);
        }
        return Optional.empty();
    }

    public static void closeWrap(final AutoCloseable closeable, final ErrorHandler errorHandler) {
        try {
            closeable.close();
        } catch (Exception e) {
            errorHandler.handleError(e, closeable);
        }
    }

    public static void closeAllInstancesOfLoadable(final Class clazz) {
        RuntimeUtil.lookup(clazz).forEachRemaining(it -> RuntimeUtil.closeWrap(((Loadable) it)));
    }

    private static void delay(final DelayType type, final Configuration config) {
        switch (type) {
            case IO_THREAD_INIT:
                try {
                    Thread.sleep(Long.parseLong(LocalParallelStreamRuntime.CONFIG
                            .getOrDefault(LocalParallelStreamRuntime.Config.Keys.DELAY_MS, config)));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                break;
        }
    }

    public static ErrorHandler loadErrorHandler(final Object context, final Configuration config) {
        final ErrorHandler eh = LoggingErrorHandler.create(context, config);
        if (ErrorHandler.trigger.get() != null)
            return eh.withTrigger(ErrorHandler.trigger.get());
        return eh;
    }

    public static Runtime.PHASE getCurrentPhase(final Configuration config) {
        return Runtime.PHASE.valueOf(config.getString(ConfigurationBase.Keys.PHASE));
    }

    public static void unload(final Class<? extends Loadable> clazz) {
        if (WorkChunkDriver.class.isAssignableFrom(clazz))
            LocalParallelStreamRuntime.workChunkDriver.set(null);
        else if (OutputIdDriver.class.isAssignableFrom(clazz))
            LocalParallelStreamRuntime.outputIdDriver.set(null);
        else if (Output.class.isAssignableFrom(clazz))
            LocalParallelStreamRuntime.outputs.clear();
        else if (Emitter.class.isAssignableFrom(clazz))
            LocalParallelStreamRuntime.emitters.clear();
        else if (Encoder.class.isAssignableFrom(clazz))
            LocalParallelStreamRuntime.encoders.clear();
        else if (Decoder.class.isAssignableFrom(clazz))
            LocalParallelStreamRuntime.decoders.clear();
        else throw new RuntimeException("Unknown class: " + clazz.getName());
    }

    public static <T> Set<Class<T>> findAvailableSubclasses(final Class<T> clazz, final String packagePrefix) {
        final ConfigurationBuilder c = new ConfigurationBuilder()
                .setClassLoaders(new ClassLoader[]{
                        ClassLoader.getPlatformClassLoader()
                })
                .setUrls(ClasspathHelper.forPackage(packagePrefix))
                .setScanners(SubTypes);
        final Reflections reflections = new Reflections(c);
        return reflections.get(Scanners.SubTypes.of(clazz).asClass()).stream().filter(it -> !it.getName().equals(clazz.getName())).map(it -> (Class<T>) it).collect(Collectors.toSet());
    }

    public static <T> Set<Class<T>> findAvailableSubclasses(final Class<T> clazz) {
        return findAvailableSubclasses(clazz, "com.aerospike.movement");
    }

    public static Class<? extends Task> getTaskClassByAlias(final String operation) {
        return Optional.ofNullable(LocalParallelStreamRuntime.taskAliases.get(operation))
                .orElseThrow(() ->
                        new RuntimeException("No task alias found for: " + operation));
    }

    public static Map<String, Class<? extends Task>> getTasks() {
        return LocalParallelStreamRuntime.taskAliases;
    }

    public static List<String> findAvailableTaskAliases() {
        findAvailableSubclasses(Task.class).forEach(it -> registerTaskAlias(it.getSimpleName(), it));
        return new ArrayList<>(LocalParallelStreamRuntime.taskAliases.keySet());
    }

    public static void registerTaskAlias(final String simpleName, final Class clazz) {
        LocalParallelStreamRuntime.taskAliases.put(simpleName, clazz);
    }

    public static Iterator<Emitable> walk(final Stream<Emitable> input, final Output writer) {
        return IteratorUtils.flatMap(input.iterator(), emitable ->
                IteratorUtils.flatMap(emitable.emit(writer).iterator(),
                        innerEmitable -> {
                            try {
                                return walk(innerEmitable.emit(writer), writer);
                            } catch (final Exception e) {
                                getErrorHandler(writer, new MapConfiguration(new HashMap<>())).handleError(e, writer);
                                return Collections.emptyIterator();
                            }
                        }));
    }


}

