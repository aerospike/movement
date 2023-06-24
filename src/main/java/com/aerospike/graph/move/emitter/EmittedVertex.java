package com.aerospike.graph.move.emitter;

import com.aerospike.graph.move.structure.EmittedId;

public interface EmittedVertex extends EmittedElement{
    EmittedId id();
}
