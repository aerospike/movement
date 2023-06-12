package com.aerospike.graph.generator.common.tinkerpop;


import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * Will use a supplied metric to apply increasing latency to operations
 */
public class ThrottledGraph extends InstrumentedGraph {

    public ThrottledGraph(final Graph wrappedGraph) {
        super(wrappedGraph);
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return null;
    }



}
