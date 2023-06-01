package com.aerospike.graph.generator.encoder.format.tinkerpop;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.EncoderUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TraversalEncoder extends Encoder<Element> {
    public static final Config CONFIG = new Config();


    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String CLEAR = "encoder.clear";
            public static final String HOST = "encoder.host";
            public static final String PORT = "encoder.port";
            public static final String REMOTE_TRAVERSAL_SOURCE_NAME = "encoder.remoteTraversalSourceName";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.REMOTE_TRAVERSAL_SOURCE_NAME, "g");
            put(Keys.CLEAR, "true");
        }};

    }


    private final GraphTraversalSource g;

    private TraversalEncoder(GraphTraversalSource g) {
        this.g = g;
    }

    public GraphTraversalSource getTraversal() {
        return g;
    }

    public static TraversalEncoder open(Configuration config) {
        final String host = CONFIG.getOrDefault(config, Config.Keys.HOST);
        final String port = CONFIG.getOrDefault(config, Config.Keys.PORT);
        final String remoteTraversalSourceName = CONFIG.getOrDefault(config, Config.Keys.REMOTE_TRAVERSAL_SOURCE_NAME);
        GraphTraversalSource g = AnonymousTraversalSource
                .traversal()
                .withRemote(DriverRemoteConnection
                        .using(host, Integer.parseInt(port), remoteTraversalSourceName));
        if (Boolean.parseBoolean(CONFIG.getOrDefault(config, Config.Keys.CLEAR)))
            g.V().drop().iterate();

        return new TraversalEncoder(g);
    }

    @Override
    public Element encodeEdge(final EmittedEdge edge) {
        final List<Object> args = edge.propertyNames().map(name -> {
            final ArrayList<Object> results = new ArrayList<Object>();
            results.add(name);
            results.add(edge.propertyValue(name));
            return results.stream();
        }).collect(Collectors.toList());

        final Vertex inV = g.V(String.valueOf(edge.toId().getId())).next();
        final Vertex outV = g.V(String.valueOf(edge.fromId().getId())).next();
        Map<Object, Object> keyVales = new HashMap<>();
        final Iterator<Object> i = args.iterator();
        while (i.hasNext()) {
            keyVales.put(i.next(), i.next());
        }
        return g
                .V(outV)
                .addE(EncoderUtil.getFieldFromEdge((EmittedEdge) edge, "~label"))
                .to(inV)
                .property(keyVales)
                .next();
    }


    @Override
    public Element encodeVertex(final EmittedVertex vertex) {
        final List<Object> args = vertex.propertyNames().map(name -> {
            final ArrayList<Object> results = new ArrayList<Object>();
            results.add(name);
            results.add(vertex.propertyValue(name));
            return results.stream();
        }).collect(Collectors.toList());

        Map<Object, Object> keyVales = new HashMap<>();
        final Iterator<Object> i = args.iterator();
        while (i.hasNext()) {
            keyVales.put(i.next(), i.next());
        }
        return g.addV(vertex.label())
                .property(T.id, vertex.id().getId())
                .property(keyVales)
                .next();
    }

    @Override
    public Element encodeVertexMetadata(final EmittedVertex vertex) {
        return null;
    }

    @Override
    public Element encodeEdgeMetadata(final EmittedEdge edge) {
        return null;
    }

    @Override
    public Element encodeVertexMetadata(final String label) {
        return null;
    }

    @Override
    public Element encodeEdgeMetadata(final String label) {
        return null;
    }


    @Override
    public String getExtension() {
        return "";
    }

    @Override
    public void close() {
        try {
            g.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
