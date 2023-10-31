/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.test.tinkerpop;

import com.aerospike.movement.tinkerpop.common.GraphProvider;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.List;

public class SharedEmptyTinkerGraphGraphProvider implements GraphProvider {
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

    @Override
    public Graph getGraph() {
        return tinkerGraph;
    }

}
