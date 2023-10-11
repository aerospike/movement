package com.aerospike.movement.output.tinkerpop;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.graph.EmittedEdge;
import com.aerospike.movement.emitter.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.core.OutputWriter;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorHandler;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.encoding.tinkerpop.GraphEncoder;
import com.aerospike.movement.encoding.tinkerpop.TraversalEncoder;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphOutput extends Loadable implements Output, OutputWriter {
    private final Encoder<Element> encoder;
    private final AtomicLong vertexMetric;
    private final AtomicLong edgeMetric;

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
            return ConfigurationUtil.getKeysFromClass(TraversalEncoder.Config.Keys.class);
        }


        public static class Keys {
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }


    public GraphOutput(final GraphEncoder encoder, final Configuration config) {
        super(Config.INSTANCE, config);
        this.encoder = encoder;
        this.vertexMetric = new AtomicLong(0);
        this.edgeMetric = new AtomicLong(0);
    }

    public static GraphOutput open(Configuration config) {
        return new GraphOutput((GraphEncoder) RuntimeUtil.loadEncoder(config), config);
    }

    public static Configuration getOutputConfig() {
        return new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.OUTPUT, GraphOutput.class.getName());
            put(ConfigurationBase.Keys.ENCODER, GraphEncoder.class.getName());
        }});
    }


    @Override
    public OutputWriter writer(Class type, Object metadata) {
        return this;
    }

    @Override
    public Map<String,Object> getMetrics() {
        return new HashMap<>() {{
            put("vertices", vertexMetric.get());
            put("edges", edgeMetric.get());
        }};
    }


    @Override
    public void writeToOutput(final Emitable item) {
        encoder.encode(item);
        incrementMetrics(item);
        vertexMetric.addAndGet(1);
    }

    private void incrementMetrics(final Emitable item) {
        if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            vertexMetric.addAndGet(1);
        }
        if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            edgeMetric.addAndGet(1);
        }
    }

    @Override
    public void init() {
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        try {
            encoder.close();
        } catch (Exception e) {
            throw errorHandler.handleError(e);
        }
    }

    @Override
    public void dropStorage() {
        ((GraphEncoder) encoder).getGraph().traversal().V().drop().iterate();
    }

    @Override
    public void init(final Configuration config) {

    }
}
