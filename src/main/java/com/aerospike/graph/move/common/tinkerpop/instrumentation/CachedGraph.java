package com.aerospike.graph.move.common.tinkerpop.instrumentation;

import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;

import java.util.*;

public class CachedGraph implements Graph {
    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String GRAPH_PROVIDER_IMPL = "graph.provider.impl";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }
    public static final TinkerPopGraphProvider.Config CONFIG = new TinkerPopGraphProvider.Config();



    private final Graph graph;
    private final LRUVertexReadThruCache cache;

    public CachedGraph(Graph graph) {
        this.graph = graph;
        this.cache = new LRUVertexReadThruCache(graph, 10000);
    }

    public static CachedGraph open(Configuration config) {
        return new CachedGraph((Graph)
                RuntimeUtil.openClassRef(CONFIG.getOrDefault(config, Config.Keys.GRAPH_PROVIDER_IMPL), config));
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
