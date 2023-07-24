package com.aerospike.movement.emitter.generator.schema;
/*
  Created by Grant Haywood grant@iowntheinter.net
  7/17/23
*/

import com.aerospike.movement.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import com.aerospike.movement.emitter.generator.schema.def.VertexSchema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class SchemaBuilder {
    public static class Keys {
        public static final String VERTEX_TYPE = "vertexType";
        public static final String CHANCES_TO_CREATE = "create.chances";
        public static final String LIKELIHOOD = "likelihood";
        public static final String VALUE_GENERATOR_IMPL = "value.generator.impl";
        public static final String VALUE_GENERATOR_ARGS = "value.generator.args";
        public static final String ENTRYPOINT = "entrypoint";
        public static final String EDGE_TYPE = "edgeType";
    }

    final Set<VertexSchema> vertexTypes;
    final Set<EdgeSchema> edgeTypes;

    public SchemaBuilder withVertexType(VertexSchema vertexSchema) {
        this.pushVertexSchema(vertexSchema);
        return this;
    }

    private SchemaBuilder() {
        this.vertexTypes = new HashSet<>();
        this.edgeTypes = new HashSet<>();
    }

    public static SchemaBuilder create() {
        return new SchemaBuilder();
    }

    private void pushVertexSchema(final VertexSchema vertexSchema) {
        vertexTypes.add(vertexSchema);
    }

    public SchemaBuilder withEdgeType(EdgeSchema edgeSchema) {
        this.pushEdgeSchema(edgeSchema);
        return this;
    }

    private void pushEdgeSchema(final EdgeSchema edgeSchema) {
        edgeTypes.add(edgeSchema);
    }

    public GraphSchema build(final String entrypointLabel) {
        final GraphSchema schema = new GraphSchema();
        schema.vertexTypes = new ArrayList<>(vertexTypes);
        schema.edgeTypes = new ArrayList<>(edgeTypes);
        schema.entrypointVertexType = entrypointLabel;
        return schema;
    }
}
