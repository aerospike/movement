package com.aerospike.movement.output.tinkerpop;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.core.OutputWriter;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.encoding.tinkerpop.TraversalEncoder;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TraversalOutput extends Loadable implements Output, OutputWriter {
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

    private TraversalOutput(final Encoder<Element> encoder, final Configuration config) {
        super(Config.INSTANCE, config);
        this.encoder = encoder;
        this.vertexMetric = new AtomicLong(0);
        this.edgeMetric = new AtomicLong(0);
    }


    public static TraversalOutput open(Configuration config) {
        return new TraversalOutput(RuntimeUtil.loadEncoder(config), config);
    }


    @Override
    public OutputWriter writer(final Class type, final Object metadata) {
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
    public void writeToOutput(final Emitable vertex) {
        encoder.encode(vertex);
        vertexMetric.addAndGet(1);
    }

    @Override
    public void init() {
    }

    @Override
    public void flush() {


    }

    @Override
    public void close() {
        ((TraversalEncoder) encoder).close();
    }

    @Override
    public void dropStorage() {
        GraphTraversalSource g = ((TraversalEncoder) encoder).getTraversal();
        g.V().drop().iterate();
    }

    @Override
    public String toString() {
        GraphTraversalSource g = ((TraversalEncoder) encoder).getTraversal();
        Long verticesWritten = g.V().count().next();
        Long edgesWritten = g.E().count().next();
        StringBuilder sb = new StringBuilder();
        sb.append("TraversalOutput: \n");
        sb.append("\n  Vertices Written: ").append(verticesWritten).append("\n");
        sb.append("\n  Edges Written: ").append(edgesWritten).append("\n");
        return sb.toString();
    }

    @Override
    public void init(final Configuration config) {

    }
}
