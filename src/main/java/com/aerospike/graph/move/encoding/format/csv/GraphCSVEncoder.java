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

    private GraphCSVEncoder(Configuration config) {
        this.config = config;
    }

    final Configuration config;

    public static GraphCSVEncoder open(final Configuration config) {
        return new GraphCSVEncoder(config);
    }


    public List<String> getVertexHeaderFields(final String label) {
        List<String> fields = new ArrayList<>();
        fields.add("~id");
        fields.add("~label");
        ((Emitter) RuntimeUtil.lookup(Emitter.class)).getAllPropertyKeysForVertexLabel(label).stream().sorted().forEach(fields::add);
        return fields;
    }

    public List<String> getEdgeHeaderFields(final String label) {
        List<String> fields = new ArrayList<>();
        fields.add("~label");
        fields.add("~from");
        fields.add("~to");
        ((Emitter) RuntimeUtil.lookup(Emitter.class)).getAllPropertyKeysForEdgeLabel(label).stream().sorted().forEach(fields::add);
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
        return getEdgeHeaderFields(edge.label()).stream()
                .map(f -> EncoderUtil.getFieldFromEdge(edge, f))
                .collect(Collectors.toList());
    }


    @Override
    public String encodeVertex(final EmittedVertex vertex) {
        return toCsvLine(toCsvFields(vertex));
    }

    @Override
    public String encodeVertexMetadata(final EmittedVertex vertex) {
        return toCsvLine(getVertexHeaderFields(vertex.label()));
    }

    @Override
    public String encodeVertexMetadata(final String label) {

        return toCsvLine(new ArrayList<>(getVertexHeaderFields(label)));
    }

    @Override
    public String encodeEdgeMetadata(final String label) {
        return toCsvLine(new ArrayList<>(getEdgeHeaderFields(label)));
    }

    @Override
    public String encodeEdgeMetadata(final EmittedEdge edge) {
        return toCsvLine(new ArrayList<>(getEdgeHeaderFields(edge.label())));
    }

    private List<String> toCsvFields(final EmittedVertex vertex) {
        return getVertexHeaderFields(vertex.label()).stream()
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
