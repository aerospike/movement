package com.aerospike.graph.move.common.tinkerpop;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class SharedEmptyTinkerGraph {


    private static final Graph tinkerGraph = TinkerGraph.open();
    private final Configuration config;

    public static Graph getInstance() {
        return tinkerGraph;
    }

    public SharedEmptyTinkerGraph(final Configuration config) {
        this.config = config;
    }


    public static Graph open(final Configuration config) {
        return tinkerGraph;
    }

}
