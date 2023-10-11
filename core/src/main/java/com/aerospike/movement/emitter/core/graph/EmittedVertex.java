package com.aerospike.movement.emitter.core.graph;

import com.aerospike.movement.structure.core.EmittedId;

public interface EmittedVertex extends EmitableGraphElement {
    EmittedId id();
}
