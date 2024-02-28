package com.aerospike.movement.tinkerpop.common;

import com.aerospike.movement.util.core.iterator.OneShotIteratorSupplier;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class RefrenceCountedSharedGraph implements Graph {
    public static final String GRAPH_IMPL = "wrapped.graph.impl";
    public static final String OPEN = "open";
    final Graph wrappedGraph;
    private static final AtomicLong refCounter = new AtomicLong();
    private static final ConcurrentHashMap<String, Graph> sharedGraphs = new ConcurrentHashMap<>();
    private final Configuration config;

    public RefrenceCountedSharedGraph(Graph wrappedGraph, Configuration config) {
        this.wrappedGraph = wrappedGraph;
        this.config = config;
    }

    public static RefrenceCountedSharedGraph from(final Function<Void, Graph> graphGetter) {
        Graph wrappedGraph = sharedGraphs.computeIfAbsent(GRAPH_IMPL, (key) -> graphGetter.apply(null));
        return new RefrenceCountedSharedGraph(wrappedGraph, wrappedGraph.configuration());
    }

    public static Graph open(final Configuration config) {
        if (!config.containsKey(GRAPH_IMPL)) {
            throw new RuntimeException(GRAPH_IMPL + " not configured");
        }
        final Graph graph = new RefrenceCountedSharedGraph(sharedGraphs.computeIfAbsent(GRAPH_IMPL, key -> {
            final Graph wrappedGraph;
            try {
                final Class<? extends Graph> graphImpl = (Class<? extends Graph>) Class.forName(config.getString(GRAPH_IMPL));
                wrappedGraph = (Graph) graphImpl.getMethod(OPEN, Configuration.class).invoke(null, config);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return wrappedGraph;
        }), config);
        refCounter.incrementAndGet();
        return graph;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        return wrappedGraph.addVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        return wrappedGraph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return wrappedGraph.compute();
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return wrappedGraph.vertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return wrappedGraph.edges(edgeIds);
    }

    @Override
    public Transaction tx() {
        return wrappedGraph.tx();
    }

    @Override
    public void close() throws Exception {
        synchronized (wrappedGraph) {
            if (refCounter.decrementAndGet() == 0) {
                wrappedGraph.close();
                sharedGraphs.remove(GRAPH_IMPL);
            }
        }
    }

    @Override
    public Variables variables() {
        return wrappedGraph.variables();
    }

    @Override
    public Configuration configuration() {
        return config;
    }
}
