package com.aerospike.graph.move.common.tinkerpop.instrumentation;


import com.aerospike.graph.move.common.tinkerpop.instrumentation.InstrumentedGraph;
import com.aerospike.graph.move.util.ErrorUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
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
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        throw ErrorUtil.unimplemented();
    }



}
