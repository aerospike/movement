package com.aerospike.graph.move.encoding.format.csv;

import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedElement;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.emitter.generator.schema.SchemaParser;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.encoding.Decoder;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.util.ErrorUtil;
import com.aerospike.graph.move.util.RuntimeUtil;
import com.aerospike.graph.move.util.StructureUtil;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.EncoderUtil;
import org.apache.commons.configuration2.Configuration;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.stream.Collectors;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphCSVEncoder implements Encoder<String> {
    public static final Generator.Config CONFIG = new Generator.Config();
    private final Emitter emitter;

    public static class Config extends ConfigurationBase {

        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String SCHEMA_FILE = Generator.Config.Keys.SCHEMA_FILE;
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }


    private final Optional<GraphSchema> optionalSchema;

    private GraphCSVEncoder(final Optional<GraphSchema> schema, Configuration config) {
        this.optionalSchema = schema;
        this.config = config;
        this.emitter = RuntimeUtil.loadEmitter(config);
    }

    final Configuration config;
    public static GraphCSVEncoder open(final Configuration config) {
        if (config.containsKey(Generator.Config.Keys.SCHEMA_FILE)) {
            final String schemaFileLocation = CONFIG.getOrDefault(config, Generator.Config.Keys.SCHEMA_FILE);
            final GraphSchema schema = SchemaParser.parse(Path.of(schemaFileLocation));
            return new GraphCSVEncoder(Optional.of(schema), config);
        }
        return new GraphCSVEncoder(Optional.empty(),config);
    }

    public VertexSchema vertexSchemaFromLabel(final String label) {
        if (!optionalSchema.isPresent())
            throw new RuntimeException("No schema present");
        final GraphSchema schema = optionalSchema.get();
        final Optional<VertexSchema> hasSchema = schema.vertexTypes.stream().filter(v -> v.label.equals(label)).findFirst();
        if (!hasSchema.isPresent())
            throw new RuntimeException("No schema found for label: " + label);
        return hasSchema.get();
    }

    public EdgeSchema edgeSchemaFromLabel(final String label) {
        if (!optionalSchema.isPresent())
            throw new RuntimeException("No schema present");
        final GraphSchema schema = optionalSchema.get();
        final Optional<EdgeSchema> hasSchema = schema.edgeTypes.stream().filter(v -> v.label.equals(label)).findFirst();
        if (!hasSchema.isPresent())
            throw new RuntimeException("No schema found for label: " + label);
        return schema.edgeTypes.stream().filter(v -> v.label.equals(label)).findFirst().get();
    }

    //@todo multi-properties
    public static List<String> getVertexHeaderFields(final EmittedVertex vertex) {
        List<String> fields = new ArrayList<>();
        fields.add("~id");
        fields.add("~label");
        fields.addAll(vertex.propertyNames().distinct().sorted().collect(Collectors.toList()));
        return fields;
    }

    public static List<String> getVertexHeaderFields(final VertexSchema vertexSchema) {
        List<String> fields = new ArrayList<>();
        fields.add("~id");
        fields.add("~label");
        vertexSchema.properties.stream().map(p -> p.name).sorted().forEach(fields::add);
        return fields;
    }

    public static List<String> getEdgeHeaderFields(final EmittedEdge edge) {
        List<String> fields = new ArrayList<>();
        fields.add("~label");
        fields.add("~from");
        fields.add("~to");
        fields.addAll(edge.propertyNames().distinct().sorted().collect(Collectors.toList()));
        return fields;
    }

    public static List<String> getEdgeHeaderFields(final EdgeSchema edgeSchema) {
        List<String> fields = new ArrayList<>();
        fields.add("~label");
        fields.add("~from");
        fields.add("~to");
        edgeSchema.properties.forEach(p -> fields.add(p.name));
        return fields;
    }


    public static String toCsvLine(final List<String> fields) {
        Optional<String> x = fields.stream().reduce((a, b) -> a + "," + b);
        return x.get();
    }

    @Override
    public String encodeEdge(final EmittedEdge edge) {
        return toCsvLine(toCsvFields(edge));
    }

    private List<String> toCsvFields(final EmittedEdge edge) {
        return getEdgeHeaderFields(edge).stream()
                .map(f -> EncoderUtil.getFieldFromEdge(edge, f))
                .collect(Collectors.toList());
    }


    @Override
    public String encodeVertex(final EmittedVertex vertex) {
        return toCsvLine(toCsvFields(vertex));
    }

    @Override
    public String encodeVertexMetadata(final EmittedVertex vertex) {
        return toCsvLine(emitter.getAllPropertyKeysForVertexLabel(vertex.label()));
    }

    @Override
    public String encodeVertexMetadata(final String label) {
        if (!optionalSchema.isPresent())
            throw new RuntimeException("No schema present");
        final GraphSchema schema = optionalSchema.get();
        return toCsvLine(new ArrayList<>(getVertexHeaderFields(StructureUtil.getSchemaFromVertexLabel(schema, label))));
    }

    @Override
    public String encodeEdgeMetadata(final String label) {
        if (!optionalSchema.isPresent())
            throw new RuntimeException("No schema present");
        final GraphSchema schema = optionalSchema.get();
        return toCsvLine(new ArrayList<>(getEdgeHeaderFields(StructureUtil.getSchemaFromEdgeLabel(schema, label))));
    }

    @Override
    public String encodeEdgeMetadata(final EmittedEdge edge) {
        return toCsvLine(new ArrayList<>(getEdgeHeaderFields(edge)));
    }

    private List<String> toCsvFields(final EmittedVertex vertex) {
        return getVertexHeaderFields(vertex).stream()
                .map(f -> String.valueOf(vertex.propertyValue(f).orElse("")))
                .collect(Collectors.toList());
    }



    public String getExtension() {
        return "csv";
    }

    @Override
    public void close() {

    }

}
