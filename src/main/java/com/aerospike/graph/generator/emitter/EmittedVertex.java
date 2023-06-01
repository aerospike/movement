package com.aerospike.graph.generator.emitter;

import com.aerospike.graph.generator.structure.EmittedId;

public interface EmittedVertex extends EmittedElement{
    EmittedId id();
}
