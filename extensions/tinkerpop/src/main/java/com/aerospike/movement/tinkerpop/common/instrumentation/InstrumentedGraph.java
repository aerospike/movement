/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common.instrumentation;

import com.aerospike.movement.config.core.ConfigurationBase;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;

public abstract class InstrumentedGraph extends WrappedGraph {
    final Graph wrappedGraph;
    public final Map<String, BiFunction> handlers;

    public InstrumentedGraph(final Graph wrappedGraph, final Configuration config, final ConfigurationBase configurationMeta, final Map<String, BiFunction> handlers) {
        super(wrappedGraph, configurationMeta, config);
        this.wrappedGraph = wrappedGraph;
        this.handlers = handlers;
    }

    public static BiFunction addVertexHandler(BiFunction<Graph, Object[], Vertex> handler) {
        return handler;
    }

    public static BiFunction verticesHandler(BiFunction<Graph, Object[], Iterator<Vertex>> handler) {
        return handler;
    }

    public static BiFunction edgesHandler(BiFunction<Graph, Object[], Iterator<Vertex>> handler) {
        return handler;
    }

    public static BiFunction closeHandler(BiFunction<Graph, Void, Void> handler) {
        return handler;
    }

    public static BiFunction variablesHandler(BiFunction<Graph, Void, Variables> handler) {
        return handler;
    }

    public static BiFunction configurationHandler(BiFunction<Graph, Void, Configuration> handler) {
        return handler;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        return (Vertex) handlers
                .getOrDefault(Methods.ADD_VERTEX, (BiFunction<Graph, Object[], Vertex>) Graph::addVertex)
                .apply(wrappedGraph, keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return wrappedGraph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return wrappedGraph.compute();
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return (Iterator<Vertex>) handlers
                .getOrDefault(Methods.VERTICES, (BiFunction<Graph, Object[], Iterator<Vertex>>) Graph::vertices)
                .apply(wrappedGraph, vertexIds);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return (Iterator<Edge>) handlers
                .getOrDefault(Methods.EDGES, (BiFunction<Graph, Object[], Iterator<Vertex>>) Graph::vertices)
                .apply(wrappedGraph, edgeIds);
    }

    @Override
    public Transaction tx() {
        return wrappedGraph.tx();
    }

    @Override
    public void onClose()  {
        handlers
                .getOrDefault(Methods.CLOSE, (BiFunction<Graph, Void, Void>) (graph, unused) -> {
                    try {
                        graph.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                })
                .apply(wrappedGraph, null);
    }

    @Override
    public Variables variables() {
        return (Variables) handlers
                .getOrDefault(Methods.VARIABLES, (BiFunction<Graph, Void, Variables>) (graph, unused) -> graph.variables())
                .apply(wrappedGraph, null);
    }

    @Override
    public Configuration configuration() {
        return (Configuration) handlers
                .getOrDefault(Methods.CONFIGURATION, (BiFunction<Graph, Void, Configuration>) (graph, unused) -> graph.configuration())
                .apply(wrappedGraph, null);
    }
}
