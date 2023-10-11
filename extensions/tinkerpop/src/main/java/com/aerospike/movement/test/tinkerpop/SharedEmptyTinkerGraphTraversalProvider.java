package com.aerospike.movement.test.tinkerpop;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class SharedEmptyTinkerGraphTraversalProvider {
    private static final Graph tinkerGraph = TinkerGraph.open();

    public static Graph getGraphInstance() {
        return tinkerGraph;
    }

    private SharedEmptyTinkerGraphTraversalProvider() {
    }

    public static GraphTraversalSource open(final Configuration config) {
        return tinkerGraph.traversal();
    }

}
