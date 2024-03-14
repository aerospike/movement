/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.test.tinkerpop;

import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.tinkerpop.common.TraversalProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class SharedEmptyTinkerGraphTraversalProvider implements TraversalProvider {


    private final Configuration config;

    private SharedEmptyTinkerGraphTraversalProvider(Configuration config) {
        this.config = config;
    }

    public static TraversalProvider open(final Configuration config) {
        return new SharedEmptyTinkerGraphTraversalProvider(config);
    }

    public static TraversalProvider open() {
        return new SharedEmptyTinkerGraphTraversalProvider(ConfigUtil.empty());
    }


    @Override
    public GraphTraversalSource getProvided(GraphProvider.GraphProviderContext ctx) {
        final Graph graph = SharedEmptyTinkerGraphGraphProvider.open(config).getProvided(ctx);
        return graph.traversal();
    }
}
