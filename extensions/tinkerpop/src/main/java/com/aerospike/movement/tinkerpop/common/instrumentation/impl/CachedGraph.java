/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common.instrumentation.impl;

import com.aerospike.movement.config.core.ConfigurationBase;

import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.google.common.cache.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;

public class CachedGraph implements Graph {
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
            public static final String CONTEXT = GraphProvider.Keys.CONTEXT;
            public static final String GRAPH_PROVIDER = "cached.graph.provider";
            public static final String CACHE_SIZE = "cached.graph.cache.size";

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.CACHE_SIZE, String.valueOf(10000L));
        }};
    }


    public final Graph graph;
    public final LoadingCache<Object, Vertex> vertexCache;
    public final Configuration config;


    public CachedGraph(final Graph graph, final Configuration config) {
        this.config = config;
        this.graph = graph;
        this.vertexCache = CacheBuilder.newBuilder()
                .maximumSize(Long.parseLong(Config.INSTANCE.getOrDefault(Config.Keys.CACHE_SIZE, config)))
                .build(new CacheLoader<>() {
                    @Override
                    public Vertex load(final Object vertexId) {
                        Vertex x = graph.vertices(vertexId).next();
                        return x;
                    }
                });
    }

    public static CachedGraph open(final Configuration config) {
        GraphProvider graphProvider = (GraphProvider) RuntimeUtil.openClassRef(Config.INSTANCE.getOrDefault(Config.Keys.GRAPH_PROVIDER, config), config);
        return new CachedGraph(graphProvider.getProvided(GraphProvider.GraphProviderContext.fromConfig(config)), config);
    }


    @Override
    public Vertex addVertex(final Object... keyValues) {
//        final Optional<Object> idOption = ElementHelper.getIdValue(keyValues);
//        idOption.ifPresent(vertexCache::invalidate);
//        final Vertex x = graph.addVertex(keyValues);
//        vertexCache.put(x.id(), x);
//        return x;
        return graph.addVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        return graph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return graph.compute();
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return IteratorUtils.stream(graph.vertices(vertexIds)).map(vertexFromBackingStore -> {
            vertexCache.put(vertexFromBackingStore.id(), vertexFromBackingStore);
            return vertexFromBackingStore;
        }).iterator();

    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        //need to wrap vertex addEdge function in order to invalidate
        return graph.edges(edgeIds);
    }

    @Override
    public Transaction tx() {
        return graph.tx();
    }

    @Override
    public void close() throws Exception {
        graph.close();
    }

    @Override
    public Variables variables() {
        return graph.variables();
    }

    @Override
    public Configuration configuration() {
        return graph.configuration();
    }

}
