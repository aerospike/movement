package com.aerospike.movement.encoding.tinkerpop;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.graph.EmittedEdge;
import com.aerospike.movement.emitter.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Collectors;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TraversalEncoder extends Loadable implements Encoder<Element> {

    private final Configuration config;

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
            public static final String CLEAR = "encoder.clear";
            public static final String TRAVERSAL_PROVIDER = "encoder.traversal.provider";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.CLEAR, "true");
        }};
    }

    public static final Config CONFIG = new Config();


    private final GraphTraversalSource g;


    protected TraversalEncoder(final GraphTraversalSource g, final Configuration config) {
        super(Config.INSTANCE, config);
        this.config = config;
        this.g = g;
    }

    public GraphTraversalSource getTraversal() {
        return g;
    }

    public static TraversalEncoder open(final Configuration config) {
        GraphTraversalSource g = (GraphTraversalSource) RuntimeUtil.openClassRef(CONFIG.getOrDefault(Config.Keys.TRAVERSAL_PROVIDER, config), config);
        return new TraversalEncoder(g, config);
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
        final Vertex inV, outV;
        final Object toId = edge.toId().getId();
        try {
            inV = g.V(toId).next();
        } catch (NoSuchElementException nse) {
            throw errorHandler.handleError(nse, "No such vertex found for edge: %s ", edge);
        }
        final Object fromId = edge.fromId().getId();
        try {
            outV = g.V(fromId).next();
        } catch (NoSuchElementException nse) {
            throw errorHandler.handleError(nse, "No such vertex found for edge: %s", edge);
        }
        Map<Object, Object> keyValues = new HashMap<>();
        final Iterator<Object> i = args.iterator();
        while (i.hasNext()) {
            keyValues.put(i.next(), i.next());
        }
        return g
                .V(outV)
                .addE(EmittedEdge.getFieldFromEdge((EmittedEdge) edge, "~label"))
                .to(inV)
                .property(keyValues)
                .next();
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
        Map<Object, Object> keyValues = new HashMap<>();
        final Iterator<Object> i = args.iterator();
        while (i.hasNext()) {
            keyValues.put(i.next(), i.next());
        }
        try {
            return g.addV(vertex.label())
                    .property(T.id, vertex.id().getId())
                    .property(keyValues)
                    .next();
        } catch (Exception e) {
            throw errorHandler.handleError(e, "Error encoding vertex: %s", vertex);
        }
    }

    @Override
    public Element encode(final Emitable item) {
        if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            return encodeVertex((EmittedVertex) item);
        } else if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            return encodeEdge((EmittedEdge) item);
        }
        throw errorHandler.error("Unknown type: %s", item.getClass().getName());
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
            g.close();
        } catch (Exception e) {
            throw errorHandler.handleError(e, this);
        }
    }
}
