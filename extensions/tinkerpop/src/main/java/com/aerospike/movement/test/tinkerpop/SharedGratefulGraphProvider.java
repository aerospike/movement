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

public class SharedGratefulGraphProvider implements GraphProvider {
    static final Graph gratefulDead = TinkerFactory.createGratefulDead();

    public SharedGratefulGraphProvider(final Configuration config) {
    }

    public static GraphProvider open(final Configuration config) {
        return new SharedGratefulGraphProvider(config);
    }


    @Override
    public Graph getProvided(GraphProviderContext ctx) {
        return gratefulDead;
    }


}
