package com.aerospike.graph.move.emitter.generator;

import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;

import java.util.Iterator;
import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class VertexContext {
    public final GraphSchema graphSchema;
    public final VertexSchema vertexSchema;
    public final Iterator<Long> idSupplier;
    Optional<EdgeGenerator.GeneratedEdge> creationInEdge;

    public VertexContext(final GraphSchema graphSchema,
                         final VertexSchema vertexSchema,
                         final Iterator<Long> idSupplier) {
        this(graphSchema, vertexSchema, idSupplier, Optional.empty());
    }

    public VertexContext(final GraphSchema graphSchema,
                         final VertexSchema vertexSchema,
                         final Iterator<Long> idSupplier,
                         final Optional<EdgeGenerator.GeneratedEdge> creationInEdge) {
        this.graphSchema = graphSchema;
        this.vertexSchema = vertexSchema;
        this.idSupplier = idSupplier;
        this.creationInEdge = creationInEdge;
    }
}
