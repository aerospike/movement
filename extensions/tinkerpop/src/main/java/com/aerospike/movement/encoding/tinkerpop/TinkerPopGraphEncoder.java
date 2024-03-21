/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.tinkerpop.common.TinkerPopGraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.error.ErrorUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;
import java.util.stream.Collectors;

import static com.aerospike.movement.emitter.core.Emitter.encodeToOutput;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TinkerPopGraphEncoder extends Loadable implements Encoder<Element> {


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
            return ConfigUtil.getKeysFromClass(Keys.class);
        }


        public static class Keys {
            public static final String GRAPH_PROVIDER = "encoder.graphProvider";
            public static final String DROP_DANGLING_EDGES = "encoder.graph.dropDanglingEdges";

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
            put(Keys.DROP_DANGLING_EDGES, String.valueOf(false));
        }};
    }

    public static final Config CONFIG = new Config();

    private final Graph graph;

    public TinkerPopGraphEncoder(final Graph graph, final Configuration config) {
        super(Config.INSTANCE, config);
        this.graph = graph;
    }

    public static TinkerPopGraphEncoder open(final Configuration config) {
        final GraphProvider graphProvider = (GraphProvider) RuntimeUtil.openClassRef(CONFIG.getOrDefault(Config.Keys.GRAPH_PROVIDER, config), config);
        final Graph graph = graphProvider.getProvided(GraphProvider.GraphProviderContext.OUTPUT);
        return new TinkerPopGraphEncoder(graph,config);
    }


    public Optional<Element> encodeEdge(final EmittedEdge edge) {
        final List<Object> args = edge.propertyNames().flatMap(name -> {
            final ArrayList<Object> results = new ArrayList<Object>();
            Optional<Object> op = edge.propertyValue(name);
            if (op.isPresent()) {
                results.add(name);
                results.add(op.get());
            }
            return results.stream();
        }).collect(Collectors.toList());
        final List<Vertex> inV = new ArrayList<>(IteratorUtils.list(graph.vertices(edge.toId().unwrap())));
        final List<Vertex> outV = new ArrayList<>(IteratorUtils.list(graph.vertices(edge.fromId().unwrap())));
        if (inV.size() > 1 || outV.size() > 1)
            throw new RuntimeException("Graph should never return more then 1 vertex for an id");

        try {
            if (inV.isEmpty() || outV.isEmpty()) {
                final String errorMessage = String.format("could not find vertex %s when creating edge", inV.isEmpty() ? edge.toId() : edge.fromId().unwrap());
                if (Boolean.parseBoolean(CONFIG.getOrDefault(Config.Keys.DROP_DANGLING_EDGES, config))) {
                    RuntimeUtil.getLogger(this).warn(errorMessage, edge);
                } else {
                    throw errorHandler.handleFatalError(new RuntimeException(errorMessage), edge);
                }
                return Optional.empty();
            } else {
                return Optional.of(outV.get(0).addEdge((String)EmittedEdge.getFieldFromEdge(edge, "~label"), inV.get(0), args.toArray()));
            }
        } catch (Exception e) {
            throw errorHandler.handleFatalError(e, edge);
        }
    }


    public Optional<Element> encodeVertex(final EmittedVertex vertex) {
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
            add(vertex.id().unwrap());
            add(T.label);
            add(vertex.label());
        }});
        Vertex x;
        try {
            x = graph.addVertex(args.toArray());
        } catch (Exception e) {
            throw errorHandler.handleFatalError(e, vertex);
        }
        return Optional.of(x);
    }


    @Override
    public Optional<Element> encode(final Emitable item) {
        if (Optional.class.isAssignableFrom(item.getClass()))
            throw new RuntimeException("optional");
        if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            return encodeEdge((EmittedEdge) item);
        } else if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            return encodeVertex((EmittedVertex) item);
        } else {
            throw errorHandler.handleFatalError(ErrorUtil.cannotEncodeException(item));
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
    public void onClose() {
        try {
            graph.close();
        } catch (Exception e) {
            throw errorHandler.handleFatalError(e);
        }
    }


    public Graph getGraph() {
        return graph;
    }

}
