/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.test.tinkerpop;

import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.tinkerpop.common.RefrenceCountedSharedGraph;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class SharedEmptyTinkerGraphGraphProvider implements GraphProvider {

    private final Configuration config;

    public SharedEmptyTinkerGraphGraphProvider(final Configuration config) {
        if (!config.containsKey(RefrenceCountedSharedGraph.Keys.PREVENT_CLOSE_PHASE_ANY))
            config.setProperty(RefrenceCountedSharedGraph.Keys.PREVENT_CLOSE_PHASE_ANY, true);
        this.config = config;
    }


    public static SharedEmptyTinkerGraphGraphProvider open() {
        return new SharedEmptyTinkerGraphGraphProvider(ConfigUtil.empty());
    }

    public static SharedEmptyTinkerGraphGraphProvider open(final Configuration config) {
        return new SharedEmptyTinkerGraphGraphProvider(config);
    }

    @Override
    public Graph getProvided(GraphProviderContext ctx) {
        return RefrenceCountedSharedGraph.from(
                (config) -> TinkerGraph.open(),
                RefrenceCountedSharedGraph.preventCloseByPhase,
                TinkerGraph.class.getName(),
                config);
    }
}
