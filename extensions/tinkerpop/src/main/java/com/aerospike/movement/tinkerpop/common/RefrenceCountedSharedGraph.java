package com.aerospike.movement.tinkerpop.common;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class RefrenceCountedSharedGraph implements Graph {
    public static final String GRAPH_IMPL = "wrapped.graph.impl";
    public static final String SHARED_GRAPH_NAME = "graph.shared.name";
    public static final String OPEN = "open";

    private static final ConcurrentHashMap<String, Graph> sharedGraphs = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, AtomicLong> refCounters = new ConcurrentHashMap<>();
    public final Graph wrappedGraph;
    private final String sharedName;


    public RefrenceCountedSharedGraph(final Graph wrappedGraph, final String sharedName) {
        this.wrappedGraph = wrappedGraph;
        this.sharedName = sharedName;
    }


    public static Graph graphByConfiguration(final Configuration config) {
        final Graph wrappedGraph;
        try {
            final Class<? extends Graph> graphImpl = (Class<? extends Graph>) Class.forName(config.getString(GRAPH_IMPL));
            wrappedGraph = (Graph) graphImpl.getMethod(OPEN, Configuration.class).invoke(null, config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return wrappedGraph;
    }

    public static Graph getExistingOrCreate(final String sharedName, Function<String, Graph> getter) {
        synchronized (RefrenceCountedSharedGraph.class) {
            final Graph x = sharedGraphs.computeIfAbsent(sharedName, (key) -> {
                refCounters.put(sharedName, new AtomicLong(0));
                return getter.apply(key);
            });
            final AtomicLong ref = refCounters.get(sharedName);
            long count = ref.incrementAndGet();
            return x;
        }
    }

    public static Graph open(final Configuration config) {
        if (!config.containsKey(GRAPH_IMPL)) {
            throw new RuntimeException(GRAPH_IMPL + " not configured");
        }
        final String sharedName = config.containsKey(SHARED_GRAPH_NAME) ? config.getString(SHARED_GRAPH_NAME) : config.getString(GRAPH_IMPL);
        final Graph wrappedGraph = getExistingOrCreate(sharedName, (key) -> graphByConfiguration(config));
        return new RefrenceCountedSharedGraph(wrappedGraph, sharedName);
    }

    public static Graph from(Function<Configuration, Graph> graphGetter, final String sharedName, Configuration config) {
        final Graph wrappedGraph = getExistingOrCreate(sharedName, (name) -> graphGetter.apply(config));
        final RefrenceCountedSharedGraph graph = new RefrenceCountedSharedGraph(wrappedGraph, sharedName);
        return graph;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        return wrappedGraph.addVertex(keyValues);
    }

    @Override
    public Vertex addVertex(String label) {
        return wrappedGraph.addVertex(label);
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
    public <C extends TraversalSource> C traversal(Class<C> traversalSourceClass) {
        return wrappedGraph.traversal(traversalSourceClass);
    }

    @Override
    public GraphTraversalSource traversal() {
        return wrappedGraph.traversal();
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
    public <Tx extends Transaction> Tx tx(Class<Tx> txClass) {
        return wrappedGraph.tx(txClass);
    }

    @Override
    public void close() throws Exception {
        synchronized (RefrenceCountedSharedGraph.class) {
            if (refCounters.containsKey(sharedName)) {
                AtomicLong refCounter = refCounters.get(sharedName);
                if (refCounter.get() <= 0)
                    throw new RuntimeException("Reference count is : " + refCounter.get());
                long value = refCounter.decrementAndGet();
                if (value == 0) {
                    wrappedGraph.close();
                    sharedGraphs.remove(sharedName);
                    refCounters.remove(sharedName);
                }
            } else {
                throw new RuntimeException("attempted to close an already closed graph: " + sharedName);
            }
        }
    }

    @Override
    public Variables variables() {
        return wrappedGraph.variables();
    }

    @Override
    public Configuration configuration() {
        return wrappedGraph.configuration();
    }

    @Override
    public ServiceRegistry getServiceRegistry() {
        return wrappedGraph.getServiceRegistry();
    }

    @Override
    public Features features() {
        return wrappedGraph.features();
    }
}
