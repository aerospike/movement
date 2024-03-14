/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common.instrumentation.impl;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.tinkerpop.common.instrumentation.InstrumentedGraph;
import com.aerospike.movement.tinkerpop.common.instrumentation.WrappedGraph;

import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class ThrottledGraph extends InstrumentedGraph {
    private Configuration config;

    @Override
    public void init(Configuration config) {

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
            return ConfigUtil.getKeysFromClass(Config.Keys.class);
        }

        public static class Keys {
            public static final String GRAPH_PROVIDER_IMPL = WrappedGraph.Keys.GRAPH_PROVIDER_IMPL;
            public static final String DELAY_TIME = "output.graph.provider.delay";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.DELAY_TIME, String.valueOf(1000)); //1 second delay
        }};
    }

    public ThrottledGraph(final Graph wrappedGraph, final Configuration config) {
        super(wrappedGraph, config, Config.INSTANCE, new HashMap<>() {{
            put(Methods.ADD_VERTEX, addVertexHandler(new BiFunction<Graph, Object[], Vertex>() {
                @Override
                public Vertex apply(final Graph graph, final Object[] keyValues) {
                    RuntimeUtil.stall(getStallTime(Methods.ADD_VERTEX, config));
                    return graph.addVertex(keyValues);
                }
            }));
            put(Methods.VERTICES, verticesHandler(new BiFunction<Graph, Object[], Iterator<Vertex>>() {
                @Override
                public Iterator<Vertex> apply(final Graph graph, final Object[] vertexIds) {
                    RuntimeUtil.stall(getStallTime(Methods.VERTICES, config));
                    return graph.vertices(vertexIds);
                }
            }));
            put(Methods.EDGES, edgesHandler(new BiFunction<Graph, Object[], Iterator<Vertex>>() {
                @Override
                public Iterator<Vertex> apply(final Graph graph, final Object[] edgeIds) {
                    RuntimeUtil.stall(getStallTime(Methods.EDGES, config));
                    return graph.vertices(edgeIds);
                }
            }));
        }});
    }

    public static ThrottledGraph open(final Configuration config) {
        final String graphProviderName = Config.INSTANCE.getOrDefault(Config.Keys.GRAPH_PROVIDER_IMPL,config);
        final GraphProvider graphProvider = (GraphProvider) RuntimeUtil.openClassRef(graphProviderName,config);
        return new ThrottledGraph(graphProvider.getProvided(GraphProvider.GraphProviderContext.fromConfig(config)),config);
    }

    private static long getStallTime(final String methodName, final Configuration config) {
        return Long.parseLong(Config.INSTANCE.getOrDefault(Config.Keys.DELAY_TIME, config));
    }
}
