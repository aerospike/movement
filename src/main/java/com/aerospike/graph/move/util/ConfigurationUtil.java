package com.aerospike.graph.move.util;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Map;
import java.util.stream.Collectors;

public class ConfigurationUtil {
    public static Configuration configurationWithOverrides(Configuration config, Map<String, Object> overrides) {
        return new MapConfiguration(IteratorUtils.stream(config.getKeys())
                .map(key -> Map.entry(key, overrides.containsKey(key) ? overrides.get(key) : config.getProperty(key)))
                .collect(Collectors.toMap(t -> t.getKey(), x -> x.getValue())));
    }
}
