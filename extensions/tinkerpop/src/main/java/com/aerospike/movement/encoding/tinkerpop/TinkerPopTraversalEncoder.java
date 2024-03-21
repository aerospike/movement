/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.tinkerpop;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.tinkerpop.common.TraversalProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Collectors;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TinkerPopTraversalEncoder extends Loadable implements Encoder<Element> {

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
            return ConfigUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String CLEAR = "encoder.clear";
            public static final String TRAVERSAL_PROVIDER = "encoder.traversal.provider";
            public static final String DROP_DANGLING_EDGES = "encoder.traversal.dropDanglingEdges";

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.CLEAR, "false");
            put(Keys.DROP_DANGLING_EDGES, String.valueOf(false));
        }};
    }

    public static final Config CONFIG = new Config();


    private final GraphTraversalSource g;


    protected TinkerPopTraversalEncoder(final GraphTraversalSource g, final Configuration config) {
        super(Config.INSTANCE, config);
        this.config = config;
        this.g = g;
    }

    public GraphTraversalSource getTraversal() {
        return g;
    }

    public static TinkerPopTraversalEncoder open(final Configuration config) {
        final GraphTraversalSource g = ((TraversalProvider) RuntimeUtil.openClassRef(CONFIG.getOrDefault(Config.Keys.TRAVERSAL_PROVIDER, config), config)).getProvided(GraphProvider.GraphProviderContext.OUTPUT);
        TinkerPopTraversalEncoder encoder = new TinkerPopTraversalEncoder(g, config);
        RuntimeUtil.getLogger(encoder).debug("using config:" + config);
        RuntimeUtil.getLogger(encoder).debug("using graph traversal source g:" + g);
        return encoder;
    }

    public Optional<Element> encodeEdge(final EmittedEdge edge) {

        final List<Object> args = edge.propertyNames().flatMap(name -> {
            final ArrayList<Object> results = new ArrayList<Object>();
            final Optional<Object> op = edge.propertyValue(name);
            if (op.isPresent()) {
                results.add(name);
                results.add(op.get());
            }
            return results.stream();
        }).collect(Collectors.toList());
        final Vertex inV, outV;
        final Object toId = edge.toId().unwrap();
        final boolean danglingSupport = Boolean.parseBoolean(CONFIG.getOrDefault(Config.Keys.DROP_DANGLING_EDGES, config));
        try {
            inV = g.V(toId).next();
        } catch (NoSuchElementException nse) {
            final String errorMessage = String.format("could not find vertex %s when creating edge", toId);
            if (danglingSupport) {
                RuntimeUtil.getLogger(this).warn(errorMessage, edge);
                return Optional.empty();
            } else {
                throw errorHandler.handleFatalError(nse, errorMessage);
            }
        }
        final Object fromId = edge.fromId().unwrap();
        try {
            outV = g.V(fromId).next();
        } catch (NoSuchElementException nse) {
            final String errorMessage = String.format("could not find vertex %s when creating edge", fromId);
            if (danglingSupport) {
                RuntimeUtil.getLogger(this).warn(errorMessage, edge);
                return Optional.empty();
            } else {
                throw errorHandler.handleFatalError(nse, errorMessage);
            }
        }
        final Map<Object, Object> keyValues = new HashMap<>();
        final Iterator<Object> i = args.iterator();
        while (i.hasNext()) {
            keyValues.put(i.next(), i.next());
        }
        final Edge x = g
                .V(outV)
                .addE((String)EmittedEdge.getFieldFromEdge(edge, "~label"))
                .to(inV)
                .property(keyValues)
                .next();
        return Optional.of(x);
    }


    public Optional<Element> encodeVertex(final EmittedVertex vertex) {
        final List<Object> args = vertex.propertyNames().flatMap(name -> {
            final ArrayList<Object> results = new ArrayList<Object>();
            final Optional<Object> op = vertex.propertyValue(name);
            if (op.isPresent()) {
                results.add(name);
                results.add(op.get());
            }
            return results.stream();
        }).collect(Collectors.toList());
        final Map<Object, Object> keyValues = new HashMap<>();
        final Iterator<Object> i = args.iterator();
        while (i.hasNext()) {
            keyValues.put(i.next(), i.next());
        }
        try {
            final Vertex x = g.addV(vertex.label())
                    .property(T.id, vertex.id().unwrap())
                    .property(keyValues)
                    .next();
            RuntimeUtil.getLogger(this).debug("wrote vertex: " + x);
            return Optional.of(x);
        } catch (Exception e) {
            RuntimeUtil.getLogger(this).warn(e);
            RuntimeUtil.getLogger(this).warn("Error encoding vertex: %s", vertex);
        }
        return Optional.empty();
    }

    @Override
    public Optional<Element> encode(final Emitable item) {
        if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            RuntimeUtil.getLogger(this).debug("encoding vertex" + ((EmittedVertex) item).id().unwrap());
            return encodeVertex((EmittedVertex) item);
        } else if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            RuntimeUtil.getLogger(this).debug("encoding edge[" + ((EmittedEdge) item).fromId().unwrap() + ":" + ((EmittedEdge) item).toId().unwrap() + "]");
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
    public void onClose() {
        try {
            g.close();
        } catch (Exception e) {
            throw errorHandler.handleError(e, this);
        }
    }
}
