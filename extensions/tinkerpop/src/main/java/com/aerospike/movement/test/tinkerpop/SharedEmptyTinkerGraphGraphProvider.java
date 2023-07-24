package com.aerospike.movement.test.tinkerpop;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class SharedEmptyTinkerGraphGraphProvider {


    private static final Graph tinkerGraph = TinkerGraph.open();
    private final Configuration config;

    public static Graph getInstance() {
        return tinkerGraph;
    }

    public SharedEmptyTinkerGraphGraphProvider(final Configuration config) {
        this.config = config;
    }


    public static Graph open(final Configuration config) {
        return tinkerGraph;
    }

}
