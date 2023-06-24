package com.aerospike.graph.move.structure;

import com.aerospike.graph.move.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;

import java.util.NoSuchElementException;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Util {

    public static EdgeSchema getSchemaFromEdgeName(final GraphSchema schema, final String edgeTypeName) {
        return schema.edgeTypes.stream()
                .filter(edgeSchema ->
                        edgeSchema.name.equals(edgeTypeName)).findFirst()
                .orElseThrow(() -> new NoSuchElementException("No edge type found for " + edgeTypeName));
    }

    public static EdgeSchema getSchemaFromEdgeLabel(final GraphSchema schema, final String edgeTypeLabel) {
        return schema.edgeTypes.stream()
                .filter(edgeSchema ->
                        edgeSchema.label.equals(edgeTypeLabel)).findFirst()
                .orElseThrow(() -> new NoSuchElementException("No edge type found for " + edgeTypeLabel));
    }

    public static VertexSchema getSchemaFromVertexName(final GraphSchema schema, final String vertexTypeName) {
        return schema.vertexTypes.stream()
                .filter(vertexSchema ->
                        vertexSchema.name.equals(vertexTypeName)).findFirst()
                .orElseThrow(() -> new NoSuchElementException("No vertex type found for " + vertexTypeName));
    }

    public static VertexSchema getSchemaFromVertexLabel(final GraphSchema schema, final String vertexTypeLabel) {
        return schema.vertexTypes.stream()
                .filter(vertexSchema ->
                        vertexSchema.label.equals(vertexTypeLabel)).findFirst()
                .orElseThrow(() -> new NoSuchElementException("No vertex type found for " + vertexTypeLabel));
    }

    public static boolean coinFlip(final double weight) {
        return Math.random() < weight;
    }


    public static EdgeSchema getStitchSchema(GraphSchema schema) {
        return getSchemaFromEdgeName(schema, schema.stitchType);
    }
}
