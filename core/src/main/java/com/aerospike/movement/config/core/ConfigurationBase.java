/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.config.core;

import com.aerospike.movement.util.core.Builder;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.util.*;

public abstract class ConfigurationBase {


    public static final String REGEX_TO_SPLIT_ON_DOT = "\\.";

    private static class None extends ConfigurationBase {
        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return new HashMap<>();
        }

        @Override
        public List<String> getKeys() {
            return new ArrayList<>();
        }
    }

    public static final ConfigurationBase NONE = new None();


    public static class Keys {
        public static final String DOT = ".";
        public static final String ONE = "one";
        public static final String TWO = "two";

        public static final String PHASE = "phase";
        public static final String EMITTER = "emitter";
        public static final String DECODER = "decoder";
        public static final String ENCODER = "encoder";
        public static final String OUTPUT = "output";
        public static final String INTERNAL = "internal";

        public static final String OUTPUT_ID_DRIVER = OUTPUT + DOT + "idDriver";
        public static final String WORK_CHUNK_DRIVER_PREFIX = EMITTER + DOT + "workChunkDriver";

        public static final String WORK_CHUNK_DRIVER_PHASE_ONE = WORK_CHUNK_DRIVER_PREFIX + DOT + PHASE + ONE;
        public static final String WORK_CHUNK_DRIVER_PHASE_TWO = WORK_CHUNK_DRIVER_PREFIX + DOT + PHASE + TWO;
        public static final String EMITTER_PHASE_ONE = ConfigurationUtil.keyFromPathElements(EMITTER, PHASE, ONE);
        public static final String EMITTER_PHASE_TWO = ConfigurationUtil.keyFromPathElements(EMITTER, PHASE, TWO);

        public static final String INTERNAL_PHASE_INDICATOR = INTERNAL + DOT + PHASE;
        public static final String PHASE_OVERRIDE = INTERNAL + DOT + "phaseOverride";

    }

    public static Iterator<String> keysWithPrefix(final String prefix, final Configuration config) {
        return IteratorUtils.stream(config.getKeys()).filter(key -> key.startsWith(prefix)).iterator();
    }

    public <T> Optional<T> getBuilderOption(final String key, final Configuration config) {
        if (config.containsKey(Builder.postfixKey(key)))
            return Optional.of((T) Builder.Storage.get(UUID.fromString(config.getString(key)), key));
        return Optional.empty();
    }

    public <T> T getOrDefault(final String key, final Map<String, Object> config) {
        return getOrDefault(key, new MapConfiguration(config));
    }

    public <T> T getOrDefault(final String key, final Configuration config) {
        final Optional<String> it = Optional.ofNullable(
                config.containsKey(key) ? config.getString(key) : defaultConfigMap().get(key));
        if (it.isPresent())
            return (T) it.get();
        throw RuntimeUtil
                .getErrorHandler(this, config)
                .handleError(new RuntimeException("Missing required configuration key: " + key));
    }

    public abstract Map<String, String> defaultConfigMap(final Map<String, Object> config);

    public final Map<String, String> defaultConfigMap() {
        return defaultConfigMap(new HashMap<>());
    }

    public Configuration defaults(Configuration config) {
        return new MapConfiguration(defaultConfigMap(ConfigurationUtil.toMap(config)));
    }

    public Configuration defaults() {
        return new MapConfiguration(defaultConfigMap());
    }

    public abstract List<String> getKeys();

}
