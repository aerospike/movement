package com.aerospike.graph.move.process.operations;


import com.aerospike.graph.move.common.tinkerpop.instrumentation.TinkerPopGraphProvider;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.encoding.format.tinkerpop.GraphEncoder;
import com.aerospike.graph.move.output.tinkerpop.GraphOutput;
import com.aerospike.graph.move.process.Job;
import org.apache.commons.configuration2.Configuration;
import com.aerospike.graph.move.util.ErrorUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Generate extends Job {
    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String GRAPH_IMPL = TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL;
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(GraphEncoder.Config.Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
        }};
    }
    private Generate(Configuration config) {
        super(config);
    }

    public static Generate open(Configuration config) {
        return new Generate(config);
    }

    public static Map<String, Object> getBaseConfig() {
        return new HashMap<>() {{
            put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
            put(ConfigurationBase.Keys.ENCODER, GraphEncoder.class.getName());
            put(GraphEncoder.Config.Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, GraphOutput.class.getName());
        }};
    }


    @Override
    public Map<String, Object> getMetrics() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public boolean succeeded() {
        return false;
    }

    @Override
    public boolean failed() {
        return false;
    }

//    public static Generate
}
