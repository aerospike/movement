package com.aerospike.graph.move.encoding.format.tinkerpop;

import com.aerospike.graph.move.common.tinkerpop.GraphProvider;
import com.aerospike.graph.move.common.tinkerpop.instrumentation.TinkerPopGraphProvider;
import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.EncoderUtil;
import com.aerospike.graph.move.util.ErrorUtil;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Collectors;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphEncoder implements Encoder<Element> {
    public static final Config CONFIG = new Config();

    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String GRAPH_PROVIDER = "encoder.graph.provider";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
        }};
    }


    private final Graph graph;

    public GraphEncoder(final Graph graph) {
        this.graph = graph;
    }

    public static GraphEncoder open(final Configuration config) {
        Object x = RuntimeUtil.openClassRef(CONFIG.getOrDefault(config, Config.Keys.GRAPH_PROVIDER), config);
        if(Graph.class.isAssignableFrom(x.getClass())) {
            return new GraphEncoder((Graph) x);
        }else if (GraphProvider.class.isAssignableFrom(x.getClass())) {
            return new GraphEncoder(((GraphProvider) x).getGraph());
        }else{
            throw new RuntimeException("GraphEncoder Could not open graph provider");
        }
    }

    @Override
    public Element encodeEdge(final EmittedEdge edge) {
        final List<Object> args = edge.propertyNames().flatMap(name -> {
            final ArrayList<Object> results = new ArrayList<Object>();
            Optional<Object> op = edge.propertyValue(name);
            if (op.isPresent()) {
                results.add(name);
                results.add(op.get());
            }
            return results.stream();
        }).collect(Collectors.toList());
        final Vertex inV = graph.vertices(edge.toId().getId()).next();
        final Vertex outV = graph.vertices(edge.fromId().getId()).next();
        return outV.addEdge(EncoderUtil.getFieldFromEdge(edge, "~label"), inV, args.toArray());
    }


    @Override
    public Element encodeVertex(final EmittedVertex vertex) {
        final List<Object> args = vertex.propertyNames().flatMap(name -> {
            final ArrayList<Object> results = new ArrayList<Object>();
            Optional<Object> op = vertex.propertyValue(name);
            if (op.isPresent()) {
                results.add(name);
                results.add(op.get());
            }
            return results.stream();
        }).collect(Collectors.toList());
        args.addAll(0, new ArrayList() {{
            add(T.id);
            add(vertex.id().getId());
            add(T.label);
            add(vertex.label());
        }});
        Vertex x;
        try {
            x = graph.addVertex(args.toArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return x;
    }

    @Override
    public Element encodeVertexMetadata(final EmittedVertex vertex) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Element encodeEdgeMetadata(final EmittedEdge edge) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Element encodeVertexMetadata(final String label) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Element encodeEdgeMetadata(final String label) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public String getExtension() {
        return "";
    }

    public Graph getGraph() {
        return graph;
    }

    @Override
    public void close() {
        try {
            graph.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
