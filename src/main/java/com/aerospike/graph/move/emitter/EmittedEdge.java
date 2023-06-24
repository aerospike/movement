package com.aerospike.graph.move.emitter;

import com.aerospike.graph.move.structure.EmittedId;

public interface EmittedEdge extends EmittedElement{
    EmittedId fromId();
    EmittedId toId();
}
