/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common;

import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.config.core.ConfigurationBase;

import com.aerospike.movement.tinkerpop.common.instrumentation.impl.CachedGraph;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TinkerPopGraphProvider implements GraphProvider {
    public static final Config CONFIG = new Config();
    private Map<String, List<String>> vertexLabelCache = new ConcurrentHashMap<>();
    private Map<String, List<String>> edgeLabelCache = new ConcurrentHashMap<>();


    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String,Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String GRAPH_IMPL = "graph.provider.impl";
            public static final String CACHE = "graph.provider.cache";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.CACHE, "true");
        }};
    }

    private final Configuration config;
    protected final Graph graph;

    public TinkerPopGraphProvider(final Configuration config) {
        this.config = config;
        if (config.containsKey(Config.Keys.CACHE) && config.getBoolean(Config.Keys.CACHE)) {
            this.graph = new CachedGraph((Graph) RuntimeUtil.openClassRef(CONFIG.getOrDefault(Config.Keys.GRAPH_IMPL, config), config), config);
        } else {
            this.graph = (Graph) RuntimeUtil.openClassRef(CONFIG.getOrDefault(Config.Keys.GRAPH_IMPL, config), config);
        }
    }

    public TinkerPopGraphProvider(final Configuration config, final Graph graph) {
        this.config = config;
        this.graph = graph;
    }

    public static GraphProvider open(Configuration config) {
        return new TinkerPopGraphProvider(config);
    }

    @Override
    public Graph getGraph() {
        return graph;
    }

}