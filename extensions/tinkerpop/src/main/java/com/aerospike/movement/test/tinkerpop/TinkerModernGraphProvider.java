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

import java.util.function.Function;

public class TinkerModernGraphProvider implements GraphProvider {
    static Function<Configuration, Graph> classicGraphGetter = (config) -> TinkerFactory.createClassic();

    public TinkerModernGraphProvider(final Configuration config) {
    }

    public static GraphProvider open(final Configuration config) {
        return new TinkerModernGraphProvider(config);
    }

    public static GraphProvider open() {
        return new TinkerModernGraphProvider(ConfigUtil.empty());
    }

    @Override
    public Graph getProvided(GraphProviderContext ctx) {
        return TinkerFactory.createModern();
    }

}
