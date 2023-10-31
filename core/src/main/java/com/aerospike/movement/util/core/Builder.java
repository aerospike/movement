/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public interface Builder<T> {
    String BUILDER = "builder";
    String DOT = ".";

    static String postfixKey(String key) {
        return key + DOT + BUILDER;
    }

    T build();

    Builder<T> memoize(final String key);


    class Storage {
        private static final ConcurrentHashMap<UUID, Map<String, Object>> storage = new ConcurrentHashMap<>();

        public static void set(final UUID builderId, final String key, final Object value) {
            Map<String, Object> builderData = storage.computeIfAbsent(builderId, (k) -> new ConcurrentHashMap<>());
            try {
                builderData.put(key, value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static Object get(final UUID builderId, final String key) {
            Map<String, Object> builderData = storage.computeIfAbsent(builderId, (k) -> new ConcurrentHashMap<>());
            return builderData.get(key);
        }

        public static void remove(final UUID builderId) {
            storage.remove(builderId);
        }
    }
}
