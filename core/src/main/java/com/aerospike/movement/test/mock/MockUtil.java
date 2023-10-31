/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.test.mock;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.structure.core.graph.EmitableGraphElement;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.structure.core.EmittedId;
import com.aerospike.movement.test.mock.emitter.MockEmitable;
import com.aerospike.movement.test.mock.emitter.MockEmitter;
import com.aerospike.movement.test.mock.encoder.MockEncoder;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public final class MockUtil {
    private static final ConcurrentHashMap<Class, ConcurrentHashMap<String, AtomicLong>> methodCounters = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicLong>> metadataCounters = new ConcurrentHashMap<>();

    private static final Map<Class, Map<String, MockCallback>> callbacks = new HashMap<>();
    public static final AtomicBoolean countHits = new AtomicBoolean(true);


    public static void clear() {
        callbacks.clear();
        methodCounters.clear();
    }


    public static void setDefaultMockCallbacks() {
        MockUtil.setCallback(MockEncoder.class, MockEncoder.Methods.GET_ENCODER_METADATA,
                MockCallback.create((object, args) -> {
                    incrementHitCounter(MockEncoder.class, MockEncoder.Methods.GET_ENCODER_METADATA);
                    return Optional.of(Collections.emptyMap());
                }));
        MockUtil.setCallback(MockEncoder.class, MockEncoder.Methods.ENCODE_ITEM_METADATA,
                MockCallback.create((object, args) -> {
                    incrementHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE_ITEM_METADATA);
                    return Optional.of(Optional.of("mockHeader"));
                }));
        MockUtil.setCallback(MockEmitter.class, MockEmitter.Methods.STREAM,
                MockCallback.create((object, args) -> {
                    incrementHitCounter(MockEmitter.class, MockEmitter.Methods.STREAM);
                    return Optional.of(args[0]);
                }));

        MockUtil.setCallback(MockEmitable.class, MockEmitable.Methods.EMIT,
                MockCallback.create((object, args) -> {
                    incrementHitCounter(MockEmitable.class, MockEmitable.Methods.EMIT);
                    return Optional.of(Stream.empty());
                }));

        MockUtil.setCallback(MockOutput.class, MockOutput.Methods.CLOSE,
                MockCallback.create((object, args) -> {
                    incrementHitCounter(MockOutput.class, MockOutput.Methods.CLOSE);
                    return Optional.empty();
                }));

        MockUtil.setCallback(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT,
                MockCallback.create((object, args) -> {
                    incrementHitCounter(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT);
                    return Optional.empty();
                }));

        MockUtil.setCallback(MockOutput.class, MockOutput.Methods.WRITER,
                MockCallback.create((object, args) -> {
                    incrementHitCounter(MockOutput.class, MockOutput.Methods.WRITER);
                    return Optional.of(object);
                }));

        MockUtil.setCallback(MockOutput.class, MockOutput.Methods.READER,
                MockCallback.create((object, args) -> {
                    incrementHitCounter(MockOutput.class, MockOutput.Methods.READER);
                    return Optional.of(object);
                }));

        MockUtil.setCallback(MockEncoder.class, MockEncoder.Methods.ENCODE,
                MockCallback.create((object, args) -> {
                    incrementHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE);
                    return Optional.of(args[0]);
                }));
    }

    public static void setDuplicateIdOutputVerification(final Set<Object> emittedIds, final AtomicBoolean detected) {
        MockUtil.setCallback(MockOutput.class, MockOutput.Methods.WRITER,
                MockCallback.create((object, args) -> {
                    incrementHitCounter(MockOutput.class, MockOutput.Methods.WRITER);
                    return Optional.of(object);
                }));
        MockUtil.setCallback(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT,
                MockCallback.create((object, args) -> {
                    final EmitableGraphElement item;
                    try {
                        item = (EmitableGraphElement) args[1];
                    } catch (Exception e) {
                        throw RuntimeUtil.getErrorHandler(MockUtil.class).handleError(new RuntimeException(e));
                    }
                    if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
                        final EmittedId id = ((EmittedVertex) item).id();
                        //Fail if an id is emitted twice
                        if (emittedIds.contains(id.getId())) {
                            detected.set(true);
                        }
                        emittedIds.add(id.getId());
                    }
                    incrementHitCounter(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT);
                    return Optional.empty();
                }));
    }

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Keys.class);
        }

        public static class Keys {
            public static final String CALLBACK_PREFIX = "test.callbacks";

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }


    public static Optional<Object> onEvent(final Class clazz, final String method, final Object object, Object... args) {
        return lookupCallback(clazz, method).flatMap(c -> c.onEvent(object, args));
    }

    private static Optional<MockCallback> lookupCallback(final Class clazz, final String method) {
        return Optional.ofNullable(callbacks.get(clazz)).map(m -> m.get(method));
    }

    public static void setCallback(final Class clazz, final String method, final MockCallback callback) {
        callbacks.computeIfAbsent(clazz, k -> new HashMap<>()).put(method, callback);
    }

    public static void incrementHitCounter(final Class<?> objectClass, final String getDriverForPhase) {
        if (countHits.get())
            methodCounters.computeIfAbsent(objectClass, (k) -> new ConcurrentHashMap<>()).computeIfAbsent(getDriverForPhase, (k) -> new AtomicLong(0)).incrementAndGet();
    }

    public static void incrementMetadataCounter(final String subtypeName, final String typeName) {
        if (countHits.get())
            metadataCounters
                    .computeIfAbsent(typeName, (k) -> new ConcurrentHashMap<>())
                    .computeIfAbsent(subtypeName, (k) -> new AtomicLong(0))
                    .incrementAndGet();
    }

    public static long getHitCounter(final Class<?> clazz, final String method) {
        return countHits.get() ? methodCounters.computeIfAbsent(clazz, aClass -> new ConcurrentHashMap<>()).computeIfAbsent(method, s -> new AtomicLong(0)).get() : 0L;
    }

    public static long getMetadataHitCounter(final String typeName, final String subtypeName) {
        try {
            return metadataCounters.get(typeName).get(subtypeName).get();
        } catch (Exception e) {
            throw RuntimeUtil.getErrorHandler(MockUtil.class).handleError(new RuntimeException(e));
        }
    }

    public static List<String> getMetadataTypes() {
        return IteratorUtils.asList(metadataCounters.keys().asIterator());
    }

    public static List<String> getMetadataSubtypes(String typeName) {
        return IteratorUtils.asList(metadataCounters.get(typeName).keys());
    }

}
