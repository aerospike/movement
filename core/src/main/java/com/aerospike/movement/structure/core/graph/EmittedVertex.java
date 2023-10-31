/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.structure.core.graph;

import com.aerospike.movement.structure.core.EmittedId;

public interface EmittedVertex extends EmitableGraphElement {
    EmittedId id();
}
