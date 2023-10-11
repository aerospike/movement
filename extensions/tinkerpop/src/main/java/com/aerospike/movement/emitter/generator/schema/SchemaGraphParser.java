package com.aerospike.movement.emitter.generator.schema;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.generator.ValueGeneratorConfig;
import com.aerospike.movement.emitter.generator.schema.def.*;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphGraphProvider;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.tinkerpop.SchemaGraphUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.File;
import java.nio.file.Path;
import java.util.*;


/**
 * @author Created by Grant Haywood grant@iowntheinter.net 7/17/2023
 * <a href="https://gist.github.com/okram/2aa70423e130bfc9118b4dde9c75c183">
 * Inspired by Marko Rodriguez's gist and commentary on Graph Schema
 * </a>
 */

public class SchemaGraphParser implements Parser {


    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }

        public static class Keys {
            public static final String GRAPHSON_FILE = "generator.schema.motif.graphml.file";
            public static final String GRAPH_PROVIDER = "generator.schema.motif.graph.provider";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }


    private final Graph schemaGraph;

    private SchemaGraphParser(final Graph schemaGraph) {
        this.schemaGraph = schemaGraph;
    }

    private static Graph openSchemaGraph(final Configuration config) {
        final Graph graph;
        if (config.containsKey(Config.Keys.GRAPHSON_FILE)) {
            graph = readGraphSON(Path.of(config.getString(Config.Keys.GRAPHSON_FILE)));
        } else if (config.containsKey(Config.Keys.GRAPH_PROVIDER)) {
            //@todo fix graph provider returns Graph
            graph = ((Graph) RuntimeUtil.openClassRef(config.getString(Config.Keys.GRAPH_PROVIDER), config));
        } else {
            throw new IllegalArgumentException("No GraphSON file or graph provider specified");
        }
        return graph;
    }

    public static SchemaGraphParser open(final Configuration config) {
        return new SchemaGraphParser(openSchemaGraph(config));
    }


    private VertexSchema fromTinkerPop(final Vertex tp3SchemaVertex) {
        final VertexSchema vertexSchema = new VertexSchema();
        final List<PropertySchema> propertySchemas = new ArrayList<>();
        tp3SchemaVertex.properties().forEachRemaining(tp3VertexProperty -> {
            if (isMetadata(tp3VertexProperty.key())) {
                return;
            }
            final PropertySchema propertySchema = new PropertySchema();
            propertySchema.name = tp3VertexProperty.key();
            propertySchema.type = (String) tp3VertexProperty.value();
            propertySchema.likelihood = (double) getSubKey(tp3SchemaVertex, tp3VertexProperty.key(),
                    SchemaBuilder.Keys.LIKELIHOOD).orElse(1.0);
            final ValueGeneratorConfig valueConfig = new ValueGeneratorConfig();
            final String implClassName = (String) getSubKey(tp3SchemaVertex, tp3VertexProperty.key(), SchemaBuilder.Keys.VALUE_GENERATOR_IMPL).orElseThrow(() ->
                    new IllegalArgumentException("No value generator implementation specified for property " + tp3VertexProperty.key())
            );
            final Map<String, Object> generatorArgs;
            try {
                generatorArgs = (Map<String, Object>) getSubKey(tp3SchemaVertex, tp3VertexProperty.key(), SchemaBuilder.Keys.VALUE_GENERATOR_ARGS).orElseThrow();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            valueConfig.impl = implClassName;
            valueConfig.args = generatorArgs;
            propertySchema.valueGenerator = valueConfig;
            propertySchemas.add(propertySchema);
        });
        vertexSchema.properties = propertySchemas;
        final List<OutEdgeSpec> outEdgeSpecs = new ArrayList<>();
        tp3SchemaVertex.edges(Direction.OUT).forEachRemaining(edge -> {
            final OutEdgeSpec outEdgeSpec = new OutEdgeSpec();
            outEdgeSpec.name = edge.id().toString();
            outEdgeSpec.likelihood = edge.properties(SchemaBuilder.Keys.LIKELIHOOD).hasNext() ?
                    (double) edge.properties(SchemaBuilder.Keys.LIKELIHOOD).next().value() : 1.0;
            outEdgeSpec.chancesToCreate = edge.properties(SchemaBuilder.Keys.CHANCES_TO_CREATE).hasNext() ?
                    (int) edge.properties(SchemaBuilder.Keys.CHANCES_TO_CREATE).next().value() : 1;
            outEdgeSpecs.add(outEdgeSpec);
        });
        vertexSchema.outEdges = outEdgeSpecs;
        vertexSchema.name = tp3SchemaVertex.id().toString();
        return vertexSchema;
    }

    private boolean isMetadata(final String key) {
        return isSubKey(key) || key.equals(SchemaBuilder.Keys.ENTRYPOINT);
    }

    private boolean isSubKey(final String key) {
        return key.contains(".");
    }

    private static Optional<Object> getSubKey(final Element tp3ele, final String key, final String subKey) {
        if (!tp3ele.properties(SchemaGraphUtil.subKey(key, subKey)).hasNext()) {
            return Optional.empty();
        }
        return Optional.of(tp3ele.value(SchemaGraphUtil.subKey(key, subKey)));
    }

    private EdgeSchema fromTinkerPop(final Edge tp3SchemaEdge) {
        final EdgeSchema edgeSchema = new EdgeSchema();
        edgeSchema.name = tp3SchemaEdge.id().toString();
        edgeSchema.inVertex = tp3SchemaEdge.inVertex().id().toString();
        edgeSchema.outVertex = tp3SchemaEdge.outVertex().id().toString();
        final List<PropertySchema> propertySchemas = new ArrayList<>();
        tp3SchemaEdge.properties().forEachRemaining(tp3EdgeProperty -> {
            if (isMetadata(tp3EdgeProperty.key())) {
                return;
            }
            final PropertySchema propertySchema = new PropertySchema();
            propertySchema.name = tp3EdgeProperty.key();
            propertySchema.type = (String) tp3EdgeProperty.value();
            propertySchema.likelihood = (double) getSubKey(tp3SchemaEdge, tp3EdgeProperty.key(),
                    SchemaBuilder.Keys.LIKELIHOOD).orElse(1.0);
            final ValueGeneratorConfig valueConfig = new ValueGeneratorConfig();
            final String implClassName = (String) getSubKey(tp3SchemaEdge, tp3EdgeProperty.key(),
                    SchemaBuilder.Keys.VALUE_GENERATOR_IMPL).orElseThrow();
            final Map<String, Object> generatorArgs = (Map<String, Object>) getSubKey(tp3SchemaEdge, tp3EdgeProperty.key(),
                    SchemaBuilder.Keys.VALUE_GENERATOR_ARGS).orElse(new HashMap<>());
            valueConfig.impl = implClassName;
            valueConfig.args = generatorArgs;
            propertySchema.valueGenerator = valueConfig;
            propertySchemas.add(propertySchema);
        });
        edgeSchema.properties = propertySchemas;
        return edgeSchema;
    }

    @Override
    public GraphSchema parse() {
        final SchemaBuilder builder = SchemaBuilder.create();
        schemaGraph.vertices().forEachRemaining(tp3Vertex -> {
            final VertexSchema vertexSchema = fromTinkerPop(tp3Vertex);
            builder.withVertexType(vertexSchema);
        });
        schemaGraph.edges().forEachRemaining(tp3Edge -> {
            final EdgeSchema edgeSchema = fromTinkerPop(tp3Edge);
            builder.withEdgeType(edgeSchema);
        });
        return builder.build(schemaGraph.traversal().V().has(SchemaBuilder.Keys.ENTRYPOINT, true).next().id().toString());
    }

    public static void writeGraphSON(final GraphSchema schema, final Path output) {
        final Graph graph = TinkerGraph.open();
        SchemaGraphUtil.writeToGraph(graph, schema);
        graph.traversal().io(output.toAbsolutePath().toString()).write().iterate();
    }

    public static Graph readGraphSON(final Path graphSonPath) {
        if (!graphSonPath.toFile().exists()) {
            throw new RuntimeException(graphSonPath + " file does not exist.");
        }
        final Graph graph = TinkerGraph.open();
        graph.traversal().io(graphSonPath.toAbsolutePath().toString()).read().iterate();
        return graph;
    }

    public static GraphSchema fromGraph(final Graph graph) {
        return new SchemaGraphParser(graph).parse();
    }

    public static GraphSchema fromGraphSON(final Path graphSonPath) {
        return fromGraph(readGraphSON(graphSonPath));
    }
}
