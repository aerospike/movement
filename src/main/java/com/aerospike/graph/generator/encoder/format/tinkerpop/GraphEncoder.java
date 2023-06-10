package com.aerospike.graph.generator.encoder.format.tinkerpop;

import com.aerospike.graph.generator.common.tinkerpop.GraphProvider;
import com.aerospike.graph.generator.common.tinkerpop.TinkerPopGraphProvider;
import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.output.graph.GraphOutput;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.EncoderUtil;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.*;
import java.util.stream.Collectors;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphEncoder extends Encoder<Element> {
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
        final GraphProvider provider = (GraphProvider) RuntimeUtil.openClassRef(CONFIG.getOrDefault(config, Config.Keys.GRAPH_PROVIDER), config);
        final Graph providerGraph = provider.getGraph();
        return new GraphEncoder(providerGraph);
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
