/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common;

import com.aerospike.movement.util.core.error.ErrorUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class GraphTraversalProvider implements TraversalProvider {
    @Override
    public GraphTraversalSource getTraversal() {
        throw ErrorUtil.unimplemented();
    }
}
