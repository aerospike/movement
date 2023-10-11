package com.aerospike.movement.encoding.files.csv;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.graph.EmitableGraphElement;
import com.aerospike.movement.encoding.core.Decoder;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphCSVDecoder extends Loadable implements Decoder<String> {



    @Override
    public Optional<Object> notify(final Loadable.Notification n) {
        return Optional.empty();
    }

    @Override
    public void init(final Configuration config) {

    }

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String,Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }



        public static class Keys {
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }

    public static final Config CONFIG = new Config();

    final Configuration config;



    private GraphCSVDecoder(final Configuration config) {
        super(MockOutput.Config.INSTANCE, config);
        this.config = config;
    }

    public static GraphCSVDecoder open(final Configuration config) {
        return new GraphCSVDecoder(config);
    }

    @Override
    public EmitableGraphElement decodeElement(String encodedElement, String headerLine, Runtime.PHASE phase) {
        if (phase.equals(Runtime.PHASE.ONE))
            return (EmitableGraphElement) new CSVVertex(new CSVLine(encodedElement, headerLine));
        else if (phase.equals(Runtime.PHASE.TWO))
            return (EmitableGraphElement) new CSVEdge(new CSVLine(encodedElement, headerLine));
        throw ErrorUtil.unimplemented();
    }



    @Override
    public void close() {

    }

    @Override
    public boolean skipEntry(String line) {
        return line.startsWith("~");
    }

}
