package com.aerospike.movement.emitter.generator.schema;

import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;

public interface Parser {
    GraphSchema parse();
}