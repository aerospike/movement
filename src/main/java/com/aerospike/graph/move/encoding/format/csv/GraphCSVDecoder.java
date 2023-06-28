package com.aerospike.graph.move.encoding.format.csv;

import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.EmittedElement;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.encoding.Decoder;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.util.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;


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
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }

    public static final Generator.Config CONFIG = new Generator.Config();

    final Configuration config;



    private GraphCSVDecoder(Configuration config) {
        this.config = config;
    }

    public static GraphCSVDecoder open(final Configuration config) {
        return new GraphCSVDecoder(config);
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
