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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.function.Function;

public class SharedTinkerClassicGraphProvider implements GraphProvider {
    static Function<Configuration, Graph> classicGraphGetter = (config) -> TinkerFactory.createClassic();

    public SharedTinkerClassicGraphProvider(final Configuration config) {
    }

    public static GraphProvider open(final Configuration config) {
        return new SharedTinkerClassicGraphProvider(config);
    }

    public static GraphProvider open() {
        return new SharedTinkerClassicGraphProvider(ConfigUtil.empty());
    }

    @Override
    public Graph getProvided(GraphProviderContext ctx) {
        return RefrenceCountedSharedGraph.from(classicGraphGetter,"Classic", ConfigUtil.empty());
    }

}
