/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.jvm.JarUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.aerospike.movement.tinkerpop.common.GraphProvider.Keys.INPUT;
import static com.aerospike.movement.tinkerpop.common.GraphProvider.Keys.OUTPUT;
import static com.aerospike.movement.tinkerpop.common.TinkerPopGraphProvider.Config.Keys.DEFAULTS;
import static com.aerospike.movement.tinkerpop.common.TinkerPopGraphProvider.Config.Keys._JAR_FILE;
import static com.aerospike.movement.util.core.configuration.ConfigUtil.cons;

public class TinkerPopGraphProvider implements GraphProvider {
    public static final Config CONFIG = new Config();
    private final Graph graph;
    private final Configuration config;
    private Map<String, List<String>> vertexLabelCache = new ConcurrentHashMap<>();
    private Map<String, List<String>> edgeLabelCache = new ConcurrentHashMap<>();
    private final GraphProviderContext ctx;


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
            return ConfigUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {

            protected static final String _GRAPH_IMPL = "graph.provider.impl";
            protected static final String _CACHE = "graph.provider.cache";
            protected static final String _JAR_FILE = "graph.provider.jarfile";
            public static final String INPUT_GRAPH_IMPL = cons(INPUT, _GRAPH_IMPL);
            public static final String OUTPUT_GRAPH_IMPL = cons(OUTPUT, _GRAPH_IMPL);

            public static final String INPUT_CACHE = cons(INPUT, _CACHE);
            public static final String OUTPUT_CACHE = cons(OUTPUT, _CACHE);

            public static final String INPUT_JAR_FILE = cons(INPUT, _JAR_FILE);
            public static final String OUTPUT_JAR_FILE = cons(OUTPUT, _JAR_FILE);


            public static final Map<String, String> DEFAULTS = new HashMap<>() {{
                put(Keys.INPUT_CACHE, "false");
                put(Keys.OUTPUT_CACHE, "false");

            }};
        }
    }


    public TinkerPopGraphProvider(final Configuration config) {
        this.config = config;
        ctx = GraphProviderContext
                .fromString(Optional.ofNullable(config.getString(Keys.CONTEXT))
                        .orElseThrow(() -> new RuntimeException("no context set for graph provider")));

        List<String> jarConfigs = ConfigUtil.filterBySubkeyMatchPrefixes(config, _JAR_FILE, INPUT.toString(), OUTPUT.toString());
        if (!jarConfigs.isEmpty()) {
            final String className = namespacedKey(CONFIG.getOrDefault(Config.Keys._GRAPH_IMPL, config), ctx);
            final Path jarLocation = Path.of(namespacedKey(CONFIG.getOrDefault(Config.Keys._JAR_FILE, config), ctx));
            graph = (Graph) RuntimeUtil.openClass(JarUtil.loadFromJar(className, jarLocation), config);
        } else {
            try {
                final String graphImpl = CONFIG.getOrDefault(namespacedKey(Config.Keys._GRAPH_IMPL, ctx), config);
                graph = (Graph) RuntimeUtil.openClassRef(graphImpl, config);

            } catch (Exception e) {
                throw RuntimeUtil.getErrorHandler(this).handleFatalError(e);
            }
        }
    }

    @Override
    public Graph getProvided(GraphProviderContext ctx) {
        return graph;
    }

    private static String namespacedKey(final String key, final GraphProviderContext ctx) {
        return cons(ctx.toString().toLowerCase(), key);
    }


    public static GraphProvider open(Configuration config) {
        return new TinkerPopGraphProvider(config);
    }

}
