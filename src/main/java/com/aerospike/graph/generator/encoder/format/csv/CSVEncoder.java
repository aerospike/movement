package com.aerospike.graph.generator.encoder.format.csv;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.emitter.generated.Generator;
import com.aerospike.graph.generator.emitter.generated.schema.Parser;
import com.aerospike.graph.generator.emitter.generated.schema.def.GraphSchema;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.emitter.generated.schema.def.EdgeSchema;
import com.aerospike.graph.generator.emitter.generated.schema.def.VertexSchema;
import com.aerospike.graph.generator.structure.Util;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.EncoderUtil;
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
public class CSVEncoder extends Encoder<String> {
    public static final Generator.Config CONFIG = new Generator.Config();

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

    private CSVEncoder(final GraphSchema schema) {
        this.optionalSchema = Optional.of(schema);
    }

    private CSVEncoder() {
        this.optionalSchema = Optional.empty();
    }

    public static CSVEncoder open(final Configuration config) {
        if (config.containsKey(Generator.Config.Keys.SCHEMA_FILE)) {
            final String schemaFileLocation = CONFIG.getOrDefault(config, Generator.Config.Keys.SCHEMA_FILE);
            final GraphSchema schema = Parser.parse(Path.of(schemaFileLocation));
            return new CSVEncoder(schema);
        }
        return new CSVEncoder();
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
        return fields.stream().reduce((a, b) -> a + "," + b).get();
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
        return toCsvLine(new ArrayList<>(getVertexHeaderFields(vertex)));
    }

    @Override
    public String encodeVertexMetadata(final String label) {
        if (!optionalSchema.isPresent())
            throw new RuntimeException("No schema present");
        final GraphSchema schema = optionalSchema.get();
        return toCsvLine(new ArrayList<>(getVertexHeaderFields(Util.getSchemaFromVertexLabel(schema, label))));
    }

    @Override
    public String encodeEdgeMetadata(final String label) {
        if (!optionalSchema.isPresent())
            throw new RuntimeException("No schema present");
        final GraphSchema schema = optionalSchema.get();
        return toCsvLine(new ArrayList<>(getEdgeHeaderFields(Util.getSchemaFromEdgeLabel(schema, label))));
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

    public static class CSVLine {
        private final List<Object> line;
        private final List<String> header;

        public CSVLine(String line, String headerLine) {
            final List<String> header = parseHeader(headerLine);
            this.line = parseLine(header, line);
            this.header = header;
        }

        private static List<String> parseHeader(final String header) {
            final StringTokenizer st = new StringTokenizer(header, ",");
            final List<String> keys = new ArrayList<>();
            while (st.hasMoreTokens()) {
                keys.add(st.nextToken());
            }
            return keys;
        }

        enum CSVField {
            EMPTY
        }

        private List<Object> parseLine(List<String> header, String line) {
            StringTokenizer st = new StringTokenizer(line, ",", true);
            List<Object> results = new ArrayList<>();
            for (long i = 0; i < header.size(); i++) {
                String token = st.nextToken();
                if (token.equals(",")) {
                    // empty field
                    results.add(CSVField.EMPTY);
                } else {
                    results.add(token);
                    if (st.hasMoreTokens() && !Objects.equals(st.nextToken(), ",")) {
                        throw new RuntimeException("Expected comma after field");
                    }
                }
            }
            return results;
        }

        public Object getEntry(String key) {
            try {
                return line.get(header.indexOf(key));
            } catch (IndexOutOfBoundsException e) {
                throw new RuntimeException("Key not found: " + key);
            }
        }

        public List<String> propertyNames() {
            return header.stream().filter(k -> !k.startsWith("~")).collect(Collectors.toList());
        }
    }
}
