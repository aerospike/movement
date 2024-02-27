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

public class SharedTinkerClassicGraphProvider implements GraphProvider {
    static Graph classicGraph;

    public SharedTinkerClassicGraphProvider(final Configuration config) {
        classicGraph = TinkerFactory.createClassic();
    }

    public static GraphProvider open(final Configuration config) {
        return new SharedTinkerClassicGraphProvider(config);
    }


    @Override
    public Graph getProvided(GraphProviderContext ctx) {
        return classicGraph;
    }

}
