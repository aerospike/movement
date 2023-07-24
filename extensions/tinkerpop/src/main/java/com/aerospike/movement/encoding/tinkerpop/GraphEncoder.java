package com.aerospike.movement.encoding.tinkerpop;

import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.tinkerpop.common.instrumentation.TinkerPopGraphProvider;
import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.graph.EmittedEdge;
import com.aerospike.movement.emitter.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.util.core.*;
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
public class GraphEncoder extends Loadable implements Encoder<Element> {


    @Override
    public void init(final Configuration config) {

    }

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String GRAPH_PROVIDER = "encoder.graphProvider";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
        }};
    }

    public static final Config CONFIG = new Config();

    private final Graph graph;

    public GraphEncoder(final Graph graph, final Configuration config) {
        super(Config.INSTANCE, config);
        this.graph = graph;
    }

    public static GraphEncoder open(final Configuration config) {
        Object x = RuntimeUtil.openClassRef(CONFIG.getOrDefault(Config.Keys.GRAPH_PROVIDER, config), config);
        if (Graph.class.isAssignableFrom(x.getClass())) {
            return new GraphEncoder((Graph) x, config);
        } else if (GraphProvider.class.isAssignableFrom(x.getClass())) {
            return new GraphEncoder(((GraphProvider) x).getGraph(), config);
        } else {
            throw RuntimeUtil.getErrorHandler(GraphEncoder.class, config)
                    .handleError(new RuntimeException("GraphEncoder Could not open graph provider"));
        }
    }

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
        try {
            return outV.addEdge(EmittedEdge.getFieldFromEdge(edge, "~label"), inV, args.toArray());
        } catch (Exception e) {
            throw errorHandler.handleError(e, edge);
        }
    }


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
            throw errorHandler.handleError(e, vertex);
        }
        return x;
    }


    @Override
    public Element encode(final Emitable item) {
        if (item instanceof EmittedEdge) {
            return encodeEdge((EmittedEdge) item);
        } else if (item instanceof EmittedVertex) {
            return encodeVertex((EmittedVertex) item);
        } else {
            throw errorHandler.handleError(EncoderUtil.cannotEncodeException(item));
        }
    }

    @Override
    public Optional<Element> encodeItemMetadata(final Emitable item) {
        return Optional.empty();
    }


    @Override
    public Map<String, Object> getEncoderMetadata() {
        return new HashMap<>();
    }


    @Override
    public void close() {
        try {
            graph.close();
        } catch (Exception e) {
            throw errorHandler.handleError(e);
        }
    }

    public Graph getGraph() {
        return graph;
    }

}
