/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.test.tinkerpop;

import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.tinkerpop.common.RefrenceCountedSharedGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RefSharedEmptyTinkerGraphGraphProvider implements GraphProvider {

    private final Configuration config;

    public static Graph getGraphInstance() {
        return RefrenceCountedSharedGraph.from((none) -> TinkerGraph.open());
    }

    public RefSharedEmptyTinkerGraphGraphProvider(final Configuration config) {
        this.config = config;
    }


    public static SharedEmptyTinkerGraphGraphProvider open(final Configuration config) {
        return new SharedEmptyTinkerGraphGraphProvider(config);
    }

    @Override
    public Graph getProvided(GraphProviderContext ctx) {
        return RefrenceCountedSharedGraph.from((none) -> TinkerGraph.open());
    }
}
