package com.aerospike.graph.move.encoding.format.csv;

import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedElement;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.emitter.generator.schema.SchemaParser;
import com.aerospike.graph.move.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.encoding.Decoder;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.util.EncoderUtil;
import com.aerospike.graph.move.util.ErrorUtil;
import com.aerospike.graph.move.util.RuntimeUtil;
import com.aerospike.graph.move.util.StructureUtil;
import org.apache.commons.configuration2.Configuration;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphCSVDecoder implements Decoder<String> {

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

    public static final Generator.Config CONFIG = new Generator.Config();

    final Configuration config;

    private final Emitter emitter;


    private GraphCSVDecoder(Configuration config) {
        this.config = config;
        this.emitter = RuntimeUtil.loadEmitter(config);
    }

    public static GraphCSVDecoder open(final Configuration config) {
        return new GraphCSVDecoder(config);
    }


    public List<String> getVertexHeaderFields(final String label) {
        List<String> fields = new ArrayList<>();
        fields.add("~id");
        fields.add("~label");
        fields.addAll(emitter.getAllPropertyKeysForVertexLabel(label));
        return fields;
    }


    public List<String> getEdgeHeaderFields(final String label) {
        List<String> fields = new ArrayList<>();
        fields.add("~label");
        fields.add("~from");
        fields.add("~to");
        fields.addAll(emitter.getAllPropertyKeysForEdgeLabel(label));
        return fields;
    }


    @Override
    public EmittedElement decodeElement(String encodedElement, String headerLine, Runtime.PHASE phase) {
        if (phase.equals(Runtime.PHASE.ONE))
            return new CSVVertex(new CSVLine(encodedElement, headerLine));
        else if (phase.equals(Runtime.PHASE.TWO))
            return new CSVEdge(new CSVLine(encodedElement, headerLine));
        throw ErrorUtil.unimplemented();
    }

    @Override
    public String decodeElementMetadata(EmittedElement element) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public String decodeElementMetadata(String label) {
        throw ErrorUtil.unimplemented();
    }

    public String getExtension() {
        return "csv";
    }

    @Override
    public void close() {

    }

    @Override
    public boolean skipEntry(String line) {
        return line.startsWith("~");
    }

}
