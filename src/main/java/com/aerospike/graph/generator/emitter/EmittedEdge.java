package com.aerospike.graph.generator.emitter;

import com.aerospike.graph.generator.structure.EmittedId;

public interface EmittedEdge extends EmittedElement{
    EmittedId fromId();
    EmittedId toId();
}
