package com.aerospike.movement.util.tinkerpop;
/*
  Created by Grant Haywood grant@iowntheinter.net
  7/17/23
*/

import com.aerospike.movement.emitter.generator.schema.SchemaBuilder;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class SchemaGraphUtil {
    public static void writeToGraph(final Graph graph, final GraphSchema graphSchema) {
        graphSchema.vertexTypes.forEach(vertexSchema -> {
            final Vertex v = graph.addVertex(T.label, SchemaBuilder.Keys.VERTEX_TYPE, T.id, vertexSchema.label());
            if (vertexSchema.label().equals(graphSchema.entrypointVertexType)) {
                v.property(SchemaBuilder.Keys.ENTRYPOINT, true);
            }
            vertexSchema.properties.forEach(vertexPropertySchema -> {
                v.property(vertexPropertySchema.name, vertexPropertySchema.type);
                v.property(SchemaGraphUtil.subKey(vertexPropertySchema.name, SchemaBuilder.Keys.LIKELIHOOD), vertexPropertySchema.likelihood);
                v.property(SchemaGraphUtil.subKey(vertexPropertySchema.name, SchemaBuilder.Keys.VALUE_GENERATOR_IMPL), vertexPropertySchema.valueGenerator.impl);
                v.property(SchemaGraphUtil.subKey(vertexPropertySchema.name, SchemaBuilder.Keys.VALUE_GENERATOR_ARGS), vertexPropertySchema.valueGenerator.args);
            });
        });
        graphSchema.edgeTypes.forEach(edgeSchema -> {
            final Vertex inV = graph.vertices(edgeSchema.inVertex).next();
            final Vertex outV = graph.vertices(edgeSchema.outVertex).next();
            final Edge schemaEdge = outV.addEdge(SchemaBuilder.Keys.EDGE_TYPE, inV, T.id, edgeSchema.label());
            edgeSchema.properties.forEach(edgePropertySchema -> {
                schemaEdge.property(edgePropertySchema.name, edgePropertySchema.type);
                schemaEdge.property(SchemaGraphUtil.subKey(edgePropertySchema.name, SchemaBuilder.Keys.LIKELIHOOD), edgePropertySchema.likelihood);
                schemaEdge.property(SchemaGraphUtil.subKey(edgePropertySchema.name, SchemaBuilder.Keys.VALUE_GENERATOR_IMPL), edgePropertySchema.valueGenerator.impl);
                schemaEdge.property(SchemaGraphUtil.subKey(edgePropertySchema.name, SchemaBuilder.Keys.VALUE_GENERATOR_ARGS), edgePropertySchema.valueGenerator.args);
            });
        });
    }

    public static String subKey(final String key, final String subKey) {
        return key + "." + subKey;
    }
}
