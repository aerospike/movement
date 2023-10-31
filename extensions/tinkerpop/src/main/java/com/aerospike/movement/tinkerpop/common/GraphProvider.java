/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.List;

public interface GraphProvider {
    Graph getGraph();

}
