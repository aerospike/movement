package com.aerospike.movement.tinkerpop.common.instrumentation;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class CachedGraph implements Graph {
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
            public static final String GRAPH_PROVIDER_IMPL = "graph.provider.impl";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }


    private final Graph graph;
    private final LRUVertexReadThruCache cache;

    public CachedGraph(Graph graph) {
        this.graph = graph;
        this.cache = new LRUVertexReadThruCache(graph, 10000);
    }

    public static CachedGraph open(final Configuration config) {
        return new CachedGraph((Graph)
                RuntimeUtil.openClassRef(Config.INSTANCE.getOrDefault(Config.Keys.GRAPH_PROVIDER_IMPL, config), config));
    }


    @Override
    public Vertex addVertex(Object... keyValues) {
        final Vertex x = graph.addVertex(keyValues);
        cache.updateCache(x);
        return x;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return graph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return graph.compute();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return cache.get(vertexIds).iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
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

    private class LRUVertexReadThruCache {
        private final Graph graph;
        private final Cache<Object, Vertex> cache;

        public LRUVertexReadThruCache(final Graph graph, final int size) {
            this.graph = graph;
            cache = CacheBuilder.newBuilder()
                    .maximumSize(size)
                    .build();
        }


        private Vertex updateCache(Vertex vertex) {
            cache.put(vertex.id(), vertex);
            return vertex;
        }

        public Vertex get(Object key) {
            final Optional<Vertex> x = Optional.ofNullable(cache.getIfPresent(key));
            return x.orElse(updateCache(graph.vertices(key).next()));
        }

        public List<Vertex> get(Object... keys) {
            final List<Vertex> vertices = new ArrayList<>();
            for (Object key : keys) {
                vertices.add(get(key));
            }
            return vertices;
        }

        public CacheStats getStats() {
            return cache.stats();
        }
    }
}
