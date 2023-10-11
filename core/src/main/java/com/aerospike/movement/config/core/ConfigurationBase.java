package com.aerospike.movement.config.core;

import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.util.*;

public abstract class ConfigurationBase {


    public Configuration getBaseConfig() {
        //@todo should also get dependencies
        return new MapConfiguration(defaultConfigMap());
    }


    public static class Keys {
        public static final String EMITTER = "emitter";
        public static final String DECODER = "decoder";
        public static final String ENCODER = "encoder";
        public static final String OUTPUT = "output";
        public static final String OUTPUT_ID_DRIVER = "output.idDriver";
        public static final String WORK_CHUNK_DRIVER = "emitter.workChunkDriver";

        public static final String PHASE = "internal.phase";
    }

    public static Iterator<String> keysWithPrefix(final String prefix, final Configuration config) {
        return IteratorUtils.stream(config.getKeys()).filter(key -> key.startsWith(prefix)).iterator();
    }

    public String getOrDefault(String key, Configuration config) {
        final Optional<String> it = Optional.ofNullable(config.containsKey(key) ? config.getString(key) : defaultConfigMap().get(key));
        if(it.isPresent())
            return it.get();
        throw RuntimeUtil.getErrorHandler(this, config).handleError(new RuntimeException("Missing required configuration key: " + key));
    }

    public String getOrDefault(String key, Map<String, Object> config) {
        return getOrDefault(key, new MapConfiguration(config));
    }

    public abstract Map<String, String> defaultConfigMap(Map<String, Object> config);

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

    public static final ConfigurationBase NONE = new None();

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

}
