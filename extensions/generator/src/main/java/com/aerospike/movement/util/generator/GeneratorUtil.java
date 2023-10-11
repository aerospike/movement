package com.aerospike.movement.util.generator;

import com.aerospike.movement.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import com.aerospike.movement.emitter.generator.schema.def.VertexSchema;
import com.aerospike.movement.util.core.ErrorUtil;

import java.util.*;
import java.util.stream.Collectors;

import static com.aerospike.movement.emitter.generator.Generator.getRootVertexSchema;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GeneratorUtil {

    public static EdgeSchema getSchemaFromEdgeName(final GraphSchema schema, final String edgeTypeName) {
        return schema.edgeTypes.stream()
                .filter(edgeSchema ->
                        edgeSchema.name.equals(edgeTypeName)).findFirst()
                .orElseThrow(() -> new NoSuchElementException("No edge type found for " + edgeTypeName));
    }

    public static EdgeSchema getSchemaFromEdgeLabel(final GraphSchema schema, final String edgeTypeLabel) {
        return schema.edgeTypes.stream()
                .filter(edgeSchema ->
                        edgeSchema.label().equals(edgeTypeLabel)).findFirst()
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
                        vertexSchema.label().equals(vertexTypeLabel)).findFirst()
                .orElseThrow(() -> new NoSuchElementException("No vertex type found for " + vertexTypeLabel));
    }

    public static boolean coinFlip(final double weight) {
        return Math.random() < weight;
    }


    public static EdgeSchema getStitchSchema(GraphSchema schema, String labelA, String labelB) {
        //@todo lookup from schema
//        return getSchemaFromEdgeName(schema, schema.stitchTypes);
        throw ErrorUtil.unimplemented();
    }

    public static class GeneratedElementMetric extends AbstractMap.SimpleEntry<Long, Long> {
        public GeneratedElementMetric(final Long upperBound, final Long lowerBound) {
            super(upperBound, lowerBound);
        }

        public Long upperBound() {
            return getKey();
        }

        public Long lowerBound() {
            return getValue();
        }

        public boolean deterministic() {
            return upperBound().equals(lowerBound());
        }
    }

    private static Map<EdgeSchema, Long> willCreateEdges(final GraphSchema graphSchema, final VertexSchema vertex) {
        return vertex.outEdges.stream()
                .map(es -> new AbstractMap.SimpleEntry<>(es.toEdgeSchema(graphSchema), Long.valueOf(es.chancesToCreate)))
                .collect(Collectors.toMap(a -> a.getKey(), b -> b.getValue()));
    }

    private static Map<VertexSchema, Long> willCreateVerticies(final GraphSchema graphSchema, final VertexSchema vertex) {
        final Map<EdgeSchema, Long> edges = willCreateEdges(graphSchema, vertex);
        final Map<VertexSchema, Long> vertices = new HashMap<>();
        for (Map.Entry<EdgeSchema, Long> entry : edges.entrySet()) {
            vertices.put(GeneratorUtil.getSchemaFromVertexLabel(graphSchema, entry.getKey().inVertex), entry.getValue());
        }
        return vertices;
    }

    public static Map<String, Long> convertToLabelNames(Map<VertexSchema, Long> map) {
        Map<String, Long> results = new HashMap<>();
        for (Map.Entry<VertexSchema, Long> entry : map.entrySet()) {
            results.put(entry.getKey().label(), entry.getValue());
        }
        return results;
    }

    public static Map<String, Long> verticesCreatedRecursive(final GraphSchema graphSchema, final VertexSchema vertexSchema) {
        // How many children will my vertex create
        final Map<VertexSchema, Long> a = willCreateVerticies(graphSchema, vertexSchema);
        // If none terminate and roll back up
        if (a.isEmpty())
            return convertToLabelNames(a);
        // For each child, get its
        List<Map<String, Long>> childResults = new ArrayList<>();
        for (Map.Entry<VertexSchema, Long> entry : a.entrySet()) {
            final Map<String, Long> thing = convertToLabelNames(willCreateVerticies(graphSchema, entry.getKey()));
            childResults.add(thing);
        }
        return childResults.stream().reduce((x, y) -> {
            Map<String, Long> results = new HashMap<>();
            for (Map.Entry<String, Long> entry : x.entrySet()) {
                results.put(entry.getKey(), entry.getValue() + y.getOrDefault(entry.getKey(), 0L));
            }
            return results;
        }).orElse(new HashMap<>());
    }

    public static GeneratedElementMetric vertexCountForScale(final GraphSchema schema, final Long scaleFactor) {
        final VertexSchema rootVertex = getRootVertexSchema(schema);
        final Map<String, Long> z = verticesCreatedRecursive(schema, rootVertex);

        //                  root + all children
        final Long total = (1 + z.values().stream().reduce(0L, Long::sum)) * scaleFactor;
        return new GeneratedElementMetric(total, total);
    }

    public static GeneratedElementMetric edgeCountForScale(final GraphSchema schema, final Long scaleFactor) {
        return new GeneratedElementMetric(1L, 1L);
    }
}
