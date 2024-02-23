/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.runtime;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.encoding.core.Decoder;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.logging.core.Logger;
import com.aerospike.movement.logging.core.impl.SystemLogger;
import com.aerospike.movement.output.Metered;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.error.LoggingErrorHandler;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.*;
import static org.reflections.scanners.Scanners.SubTypes;

public class RuntimeUtil {
    public static final String IO_OPS = "io_ops";

    public static ErrorHandler getErrorHandler(final Object loggedObject) {
        return getErrorHandler(loggedObject, ConfigUtil.empty());
    }

    public static ErrorHandler getErrorHandler(final Object loggedObject, final Configuration config) {
        return loadErrorHandler(loggedObject, config);
    }

    public static Logger getLogger(final Object context) {
        return new SystemLogger(context);
    }

    public static void halt() {
        LocalParallelStreamRuntime.halt();
    }


    public static void stall(final long msToSleep) {
        try {
            Thread.sleep(msToSleep);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object construct(Class providerClass, Configuration config) {
        try {
            return providerClass.getConstructor(Configuration.class).newInstance(config);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw getErrorHandler(providerClass).handleFatalError(e, config);
        }

    }

    public static int getBatchSize(final Configuration config) {
        return Integer.parseInt(LocalParallelStreamRuntime.CONFIG.getOrDefault(LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE, config));
    }

    public static void waitTask(String x) {
        waitTask(UUID.fromString(x));
    }

    public static void waitTask(UUID x) {
        Future<Map<String, Object>> z = (Future<Map<String, Object>>) LocalParallelStreamRuntime.runningTasks.get(x).get(LocalParallelStreamRuntime.FUTURE_KEY);
        try {
            z.get();
        } catch (Exception e) {
            throw getErrorHandler(z).handleFatalError(e);
        }
    }


    public static Iterator<Map<String, Object>> statusIteratorForTask(final UUID taskId) {
        return new Iterator<Map<String, Object>>() {
            @Override
            public boolean hasNext() {
                AtomicReference<Optional<RunningPhase>> x = ((AtomicReference<Optional<RunningPhase>>)
                        Optional.ofNullable(LocalParallelStreamRuntime.runningTasks.get(taskId))
                                .orElseThrow(() -> new RuntimeException("cannot find taskId " + taskId))
                                .get(LocalParallelStreamRuntime.PHASE_REF_KEY));
                return x.get().isPresent() && !x.get().get().isDone();
            }

            @Override
            public Map<String, Object> next() {
                return ((AtomicReference<Optional<RunningPhase>>)
                        Optional.ofNullable(LocalParallelStreamRuntime.runningTasks.get(taskId))
                                .orElseThrow(() -> new RuntimeException("cannot find taskId " + taskId))
                                .get(LocalParallelStreamRuntime.PHASE_REF_KEY)).get().map(phase -> phase.status().next()).orElse(Map.of());
            }
        };
    }

    public static Optional<RunningPhase> runningPhaseForTask(UUID taskId) {
        return ((AtomicReference<Optional<RunningPhase>>)
                Optional.ofNullable(LocalParallelStreamRuntime.runningTasks.get(taskId))
                        .orElseThrow(() -> new RuntimeException("cannot find taskId " + taskId))
                        .get(LocalParallelStreamRuntime.PHASE_REF_KEY)).get();
    }

    public static Optional<Function<Object, String>> getObjectPrinter() {
        return Optional.of(Object::toString);
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
            final Optional<Class<?>> maybeLoad = Optional.ofNullable(Class.forName(className));
            if (maybeLoad.isEmpty())
                getErrorHandler(RuntimeUtil.class).handleFatalError(new RuntimeException("Could not load class: " + className));
            final Class<?> x = maybeLoad.get();
            return openClass(maybeLoad.get(), config);
        } catch (Exception e) {
            throw RuntimeUtil.getErrorHandler(RuntimeUtil.class, config).handleFatalError(e);
        }
    }

    public static Object openClass(final Class clazz, final Configuration config) {
        try {
            final Method openMethod = getMethod(clazz, STATIC_OPEN_METHOD, Configuration.class);
            return openMethod.invoke(null, config);
        } catch (InvocationTargetException invocationTargetException) {
            throw RuntimeUtil.getErrorHandler(RuntimeUtil.class, config).handleFatalError(invocationTargetException.getTargetException());
        } catch (Exception e) {
            throw RuntimeUtil.getErrorHandler(RuntimeUtil.class, config).handleFatalError(e);
        }
    }


    public static Method getMethod(final Class clazz, final String method, Class<?>... parameterTypes) {
        try {
            return clazz.getMethod(method, parameterTypes);
        } catch (final NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static void invokeClassMain(final String className, final Object[] args) {
        invokeMethod(className, "main", null, args);
    }

    public static boolean blockOnBackpressure() {
        final Iterator<Object> meteredOutputs = RuntimeUtil.match(Output.class, Metered.class);
        if (meteredOutputs.hasNext()) {
            Metered x = (Metered) meteredOutputs.next();
//            x.barrier(); //@todo
            return true;
        }
        return false;
    }

    public static Class loadClassWithClassLoader(final String className, final ClassLoader classLoader) {
        try {
            return classLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw RuntimeUtil.getErrorHandler(RuntimeUtil.class).handleFatalError(new RuntimeException("Error loading class: " + className, e));
        }
    }

    public static Class loadClass(final String className, final ClassLoader classLoader) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw RuntimeUtil.getErrorHandler(RuntimeUtil.class).handleFatalError(new RuntimeException("Error loading class: " + className, e));
        }
    }

    public static Class loadClass(final String className) {
        return loadClass(className, Thread.currentThread().getContextClassLoader());
    }

    public static Optional<Object> invokeMethod(final String className, final String methodName, final Object object, final Object[] args) {
        final Class clazz = loadClass(className);
        return invokeMethod(clazz, methodName, object, args);
    }

    public static Optional<Object> invokeMethod(final Class clazz, final String methodName, final Object object, final Object[] args) {
        try {
            return Optional.ofNullable(clazz.getMethod(methodName, String[].class).invoke(object, new Object[]{args}));
        } catch (Exception e) {
            throw RuntimeUtil.getErrorHandler(RuntimeUtil.class).handleFatalError(new RuntimeException("Error invoking method: " + methodName, e));
        }
    }

    public static Object getStaticFieldValue(final Class clazz, final String fieldName) {
        try {
            return clazz.getField(fieldName).get(null);
        } catch (Exception e) {
            throw RuntimeUtil.getErrorHandler(RuntimeUtil.class).handleFatalError(new RuntimeException("Error getting field: " + fieldName, e));
        }
    }

    public static Class loadInnerClass(final String outerClassName, final String innerClassName) {
        final String name = (String.format("%s$%s", outerClassName, innerClassName));
        try {
            return Class.forName(name);
        } catch (Exception e) {
            throw RuntimeUtil.getErrorHandler(RuntimeUtil.class).handleFatalError(new RuntimeException("Error loading class: " + name, e));
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
        final Runtime.PHASE phase = getCurrentPhase(config);
        final List<String> emitterPhaseConfigs = ConfigUtil.getSubKeys(config, EMITTER, PHASE);
        final Emitter emitterToUse;
        if (phase.equals(Runtime.PHASE.ONE) && emitterPhaseConfigs.contains(EMITTER_PHASE_ONE))
            emitterToUse = (Emitter) openClassRef(config.getString(EMITTER_PHASE_ONE), config);
        else if (phase.equals(Runtime.PHASE.TWO) && emitterPhaseConfigs.contains(EMITTER_PHASE_TWO))
            emitterToUse = (Emitter) openClassRef(config.getString(EMITTER_PHASE_TWO), config);
        else
            emitterToUse = (Emitter) openClassRef(config.getString(EMITTER), config);
        LocalParallelStreamRuntime.emitters.add(emitterToUse);
        return emitterToUse;
    }

    public static OutputIdDriver loadOutputIdDriver(final Configuration config) {
        final OutputIdDriver it = (OutputIdDriver) load(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, config);
        LocalParallelStreamRuntime.outputIdDriver.set(it);
        return it;
    }


    public static WorkChunkDriver loadWorkChunkDriver(final Configuration config) {
        final Runtime.PHASE phase = getCurrentPhase(config);
        final WorkChunkDriver it = (WorkChunkDriver) load(
                phase.equals(Runtime.PHASE.ONE) ?
                        ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE : ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_TWO,
                config);
        LocalParallelStreamRuntime.workChunkDriver.set(it);
        return it;
    }

    public static Decoder<String> loadDecoder(final Configuration config) {
        final Decoder<String> it = (Decoder<String>) load(ConfigurationBase.Keys.DECODER, config);
        LocalParallelStreamRuntime.decoders.add(it);
        return it;
    }

    public static Object load(final String configKeyOfClassName, final Configuration config) {
        final Optional<String> driverIfPresent = Optional.ofNullable(config.getString(configKeyOfClassName));
        if (driverIfPresent.isEmpty()) {
            throw getErrorHandler(RuntimeUtil.class)
                    .handleFatalError(new RuntimeException("Config key not set: " + configKeyOfClassName), configKeyOfClassName, config);
        }
        return openClassRef(driverIfPresent.get(), config);
    }

    public static <K, V> Map<K, V> mapReducer(Map<K, V> a, Map<K, V> b) {
        final HashMap<K, V> x = new HashMap<>();
        try {
            x.putAll(a);
            x.putAll(b);
        } catch (Exception e) {
            throw getErrorHandler(RuntimeUtil.class).handleFatalError(e, a, b);
        }
        return x;
    }

    private static Object nextOrLoad(final List<Loadable> existing, Callable<Object> loader, Optional<UUID> optionalId) {
        try {
            return optionalId.map(id -> existing.stream().filter(it -> (it).getId().equals(id))).orElse(Stream.empty()).findAny().orElse((Loadable) loader.call());
        } catch (Exception e) {
            throw getErrorHandler(RuntimeUtil.class, ConfigUtil.empty()).handleFatalError(e);
        }

    }

    public static Object lookupOrLoad(final Class targetClass, final Configuration config) {
        return lookupOrLoad(targetClass, config, Optional.empty());
    }

    public static Iterator<Object> match(final Object object, final Class matchingClass) {
        return IteratorUtils.filter(lookup(object.getClass()).iterator(), it -> matchingClass.isAssignableFrom(it.getClass()));
    }

    public static List lookup(final Class targetClass) {
        if (Output.class.isAssignableFrom(targetClass))
            return LocalParallelStreamRuntime.outputs;
        if (Emitter.class.isAssignableFrom(targetClass))
            return LocalParallelStreamRuntime.emitters;
        if (Encoder.class.isAssignableFrom(targetClass))
            return LocalParallelStreamRuntime.encoders;
        if (Decoder.class.isAssignableFrom(targetClass))
            return LocalParallelStreamRuntime.decoders;
        if (OutputIdDriver.class.isAssignableFrom(targetClass))
            return Optional.ofNullable(LocalParallelStreamRuntime.outputIdDriver.get()).stream().collect(Collectors.toList());
        if (WorkChunkDriver.class.isAssignableFrom(targetClass))
            return Optional.ofNullable(LocalParallelStreamRuntime.workChunkDriver.get()).stream().collect(Collectors.toList());
        else
            return Collections.emptyList();
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
            throw RuntimeUtil.getErrorHandler(RuntimeUtil.class, config).handleFatalError(new RuntimeException("Cannot find class: " + targetClass.getName()));
    }

    private static Object loadTask(final String name, final Configuration config) {
        return openClassRef(name, config);
    }


    public static class Outcome {
        final Map.Entry<Optional<Object>, Optional<Throwable>> outcome;

        public Outcome(Map.Entry<Optional<Object>, Optional<Throwable>> outcome) {
            this.outcome = outcome;
        }

        public Optional<Object> result() {
            return outcome.getKey();
        }

        public Optional<Throwable> error() {
            return outcome.getValue();
        }
    }

    public static Outcome tryCatch(final Function<Object, Object> action, final Callable<Object> argument) {
        final Optional<Object> result;
        try {
            result = Optional.ofNullable(action.apply(argument.call()));
        } catch (Exception e) {
            return new Outcome(Map.entry(Optional.empty(), Optional.of(e)));
        }
        return new Outcome(Map.entry(result, Optional.empty()));
    }

    public static <A, R> Optional<R> errorHandled(final Function<A, R> action, final Callable<A> argument) {
        final Optional<R> result;
        try {
            result = Optional.ofNullable(action.apply(argument.call()));
        } catch (Exception e) {
            throw getErrorHandler(action).handleFatalError(e, argument);
        }
        return result;
    }

    public static Optional<Object> errorHandled(final Callable<Object> action) {
        final Optional<Object> result;
        try {
            result = Optional.ofNullable(action.call());
        } catch (Exception e) {
            throw getErrorHandler(action).handleFatalError(e);
        }
        return result;
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
        System.out.printf("will close %d instances of %s\n", RuntimeUtil.lookup(clazz).size(), clazz.getSimpleName());
        RuntimeUtil.lookup(clazz).iterator().forEachRemaining(it -> RuntimeUtil.closeWrap(((Loadable) it)));
    }

    public static void delay(final DelayType type, final Configuration config) {
        switch (type) {
            case IO_THREAD_INIT:
                stall(Long.parseLong(LocalParallelStreamRuntime.CONFIG
                        .getOrDefault(LocalParallelStreamRuntime.Config.Keys.DELAY_MS, config)));
                break;
        }
    }

    private static ErrorHandler loadErrorHandler(final Object context, final Configuration config) {
        final ErrorHandler.ErrorHandlerBuilder builder = LoggingErrorHandler.builder(config).withContext(context);
        if (ErrorHandler.trigger.get() != null)
            return builder.withTrigger(ErrorHandler.trigger.get()).build();
        return builder.build();
    }

    public static Runtime.PHASE getCurrentPhase(final Configuration config) {
        return Runtime.PHASE.valueOf(
                Optional.ofNullable(config.getString(ConfigurationBase.Keys.INTERNAL_PHASE_INDICATOR))
                        .or(() -> Optional.ofNullable(config.getString(PHASE)))
                        .or(() -> Optional.of(Runtime.PHASE.ONE.name()))
                        .orElseThrow(() -> new RuntimeException("Phase Configuration Error")));
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
        else
            throw RuntimeUtil.getErrorHandler(RuntimeUtil.class).handleFatalError(new RuntimeException("Unknown class: " + clazz.getName()));
    }

    public static <T> Set<Class<T>> findAvailableSubclasses(final Class<T> clazz, final String packagePrefix) {
        final ConfigurationBuilder c = new ConfigurationBuilder()
                .setClassLoaders(new ClassLoader[]{
                        clazz.getClassLoader()
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
}

