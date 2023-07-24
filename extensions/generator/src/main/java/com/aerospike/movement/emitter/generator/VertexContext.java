package com.aerospike.movement.emitter.generator;


import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import com.aerospike.movement.emitter.generator.schema.def.VertexSchema;

import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class VertexContext {
    public final GraphSchema graphSchema;
    public final VertexSchema vertexSchema;
    public final  OutputIdDriver outputIdDriver;
    Optional<EdgeGenerator.GeneratedEdge> creationInEdge;

    public VertexContext(final GraphSchema graphSchema,
                         final VertexSchema vertexSchema,
                         final OutputIdDriver idDriver) {
        this(graphSchema, vertexSchema, idDriver, Optional.empty());
    }

    public VertexContext(final GraphSchema graphSchema,
                         final VertexSchema vertexSchema,
                         final OutputIdDriver outputIdDriver,
                         final Optional<EdgeGenerator.GeneratedEdge> creationInEdge) {
        this.graphSchema = graphSchema;
        this.vertexSchema = vertexSchema;
        this.outputIdDriver = outputIdDriver;
        this.creationInEdge = creationInEdge;
    }
}
