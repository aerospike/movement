package com.aerospike.movement.tinkerpop.common;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.List;

public interface GraphProvider {
    Graph getGraph();

    List<String> getAllPropertyKeysForVertexLabel(String label);

    List<String> getAllPropertyKeysForEdgeLabel(String label);
}
