/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common;

import com.aerospike.movement.tinkerpop.common.instrumentation.impl.ThrottledGraph;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;

public class ThrottledTraversalProvider {
    public final Graph graph;
    private final Configuration config;

    public ThrottledTraversalProvider(final Graph graph, final Configuration config) {
        this.graph = graph;
        this.config = config;
    }

    public static ThrottledTraversalProvider open(final Configuration config) {
        Map<String, String> env = System.getenv();
        final String GRAPH_PROVIDER_IMPL;
        if (env.containsKey(ThrottledGraph.Config.Keys.GRAPH_PROVIDER_IMPL)) {
            GRAPH_PROVIDER_IMPL = env.get(ThrottledGraph.Config.Keys.GRAPH_PROVIDER_IMPL);
        } else if (config.containsKey(ThrottledGraph.Config.Keys.GRAPH_PROVIDER_IMPL)) {
            GRAPH_PROVIDER_IMPL = config.getString(ThrottledGraph.Config.Keys.GRAPH_PROVIDER_IMPL);
        } else {
            throw new RuntimeException("missing configuration key " + ThrottledGraph.Config.Keys.GRAPH_PROVIDER_IMPL);
        }
        return new ThrottledTraversalProvider(ThrottledGraph.open(ConfigUtil.withOverrides(config, new HashMap<>() {{
            put(ThrottledGraph.Config.Keys.GRAPH_PROVIDER_IMPL, GRAPH_PROVIDER_IMPL);
        }})), config);
    }

    public GraphTraversalSource traversalSource() {
        return graph.traversal();
    }
}
