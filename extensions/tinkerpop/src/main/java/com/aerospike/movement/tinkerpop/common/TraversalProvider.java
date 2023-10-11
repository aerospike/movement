package com.aerospike.movement.tinkerpop.common;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public interface TraversalProvider {
    GraphTraversalSource getTraversal();
}
