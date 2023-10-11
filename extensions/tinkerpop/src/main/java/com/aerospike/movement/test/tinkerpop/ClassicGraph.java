package com.aerospike.movement.test.tinkerpop;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

public class ClassicGraph {


    static Graph classicGraph = TinkerFactory.createClassic();
    private final Configuration config;

    public ClassicGraph(final Configuration config) {
        this.config = config;
    }

    public static Graph open(final Configuration config) {
        return classicGraph;
    }

}
