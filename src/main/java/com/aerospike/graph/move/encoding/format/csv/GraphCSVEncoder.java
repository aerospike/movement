package com.aerospike.graph.move.encoding.format.csv;

import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.util.RuntimeUtil;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.EncoderUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphCSVEncoder implements Encoder<String> {
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

    public static final Config CONFIG = new Config();
    final Configuration config;


    private GraphCSVEncoder(Configuration config) {
        this.config = config;
    }

    public static GraphCSVEncoder open(final Configuration config) {
        return new GraphCSVEncoder(config);
    }

    public static String toCsvLine(final List<String> fields) {
        Optional<String> x = fields.stream().reduce((a, b) -> a + "," + b);
        return x.get();
    }


    @Override
    public String encodeEdge(final EmittedEdge edge) {
        return toCsvLine(toCsvFields(edge));
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

    @Override
    public void close() {

    }

    public List<String> getVertexHeaderFields(final String label) {
        List<String> fields = new ArrayList<>();
        fields.add("~id");
        fields.add("~label");
        ((Emitter) RuntimeUtil.lookup(Emitter.class, config)).getAllPropertyKeysForVertexLabel(label).stream().sorted().forEach(fields::add);
        return fields;
    }

    public List<String> getEdgeHeaderFields(final String label) {
        List<String> fields = new ArrayList<>();
        fields.add("~label");
        fields.add("~from");
        fields.add("~to");
        ((Emitter) RuntimeUtil.lookup(Emitter.class, config)).getAllPropertyKeysForEdgeLabel(label).stream().sorted().forEach(fields::add);
        return fields;
    }

    public String getExtension() {
        return "csv";
    }


    private List<String> toCsvFields(final EmittedEdge edge) {
        return getEdgeHeaderFields(edge.label()).stream()
                .map(f -> EncoderUtil.getFieldFromEdge(edge, f))
                .collect(Collectors.toList());
    }

    private List<String> toCsvFields(final EmittedVertex vertex) {
        return getVertexHeaderFields(vertex.label()).stream()
                .map(f -> String.valueOf(vertex.propertyValue(f).orElse("")))
                .collect(Collectors.toList());
    }


}