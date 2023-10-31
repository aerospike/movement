/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.test.tinkerpop;

import com.aerospike.movement.tinkerpop.common.GraphProvider;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

public class ClassicGraphProvider implements GraphProvider {


    static Graph classicGraph = TinkerFactory.createClassic();
    private final Configuration config;

    public ClassicGraphProvider(final Configuration config) {
        this.config = config;
    }

    public static Graph open(final Configuration config) {
        return classicGraph;
    }

    public static Graph getInstance() {
        return classicGraph;
    }

    @Override
    public Graph getGraph() {
        return classicGraph;
    }
}
