/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.tinkerpop;

import com.aerospike.movement.encoding.core.Decoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.structure.core.EmittedId;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.tinkerpop.common.TinkerPopGraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.error.ErrorUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TinkerPopGraphEncoder extends Loadable implements Encoder<Element>, Decoder<Element> {


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

    public TinkerPopGraphEncoder(final Graph graph, final Configuration config) {
        super(Config.INSTANCE, config);
        this.graph = graph;
    }

    public static TinkerPopGraphEncoder open(final Configuration config) {
        Object x = RuntimeUtil.openClassRef(CONFIG.getOrDefault(Config.Keys.GRAPH_PROVIDER, config), config);
        if (Graph.class.isAssignableFrom(x.getClass())) {
            return new TinkerPopGraphEncoder((Graph) x, config);
        } else if (GraphProvider.class.isAssignableFrom(x.getClass())) {
            return new TinkerPopGraphEncoder(((GraphProvider) x).getGraph(), config);
        } else {
            throw RuntimeUtil.getErrorHandler(TinkerPopGraphEncoder.class, config)
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
        if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            return encodeEdge((EmittedEdge) item);
        } else if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            return encodeVertex((EmittedVertex) item);
        } else {
            throw errorHandler.handleError(ErrorUtil.cannotEncodeException(item));
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
    public Emitable decodeElement(final Element encodedElement, final String label, final Runtime.PHASE phase) {
        if (Vertex.class.isAssignableFrom(encodedElement.getClass())) {
            return new EmittedVertex() {
                @Override
                public EmittedId id() {
                    return EmittedId.from(encodedElement.id());
                }

                @Override
                public Stream<String> propertyNames() {
                    return encodedElement.keys().stream();
                }

                @Override
                public Optional<Object> propertyValue(String name) {
                    return encodedElement.keys().contains(name) ?
                            Optional.of(encodedElement.property(name)) : Optional.empty();
                }

                @Override
                public String label() {
                    return encodedElement.label();
                }

                @Override
                public Stream<Emitable> stream() {
                    return Stream.empty();
                }

                @Override
                public Stream<Emitable> emit(final Output output) {
                    output.writer(EmittedVertex.class, encodedElement.label()).writeToOutput(this);
                    return stream();
                }
            };
        } else if (Edge.class.isAssignableFrom(encodedElement.getClass())) {
            return new EmittedEdge() {
                @Override
                public EmittedId fromId() {
                    return EmittedId.from(((Edge) encodedElement).outVertex().id());
                }

                @Override
                public EmittedId toId() {
                    return EmittedId.from(((Edge) encodedElement).inVertex().id());
                }

                @Override
                public Stream<String> propertyNames() {
                    return encodedElement.keys().stream();
                }

                @Override
                public Optional<Object> propertyValue(String name) {
                    return encodedElement.keys().contains(name) ?
                            Optional.of(encodedElement.property(name)) : Optional.empty();
                }

                @Override
                public String label() {
                    return encodedElement.label();
                }

                @Override
                public Stream<Emitable> stream() {
                    return Stream.empty();
                }

                @Override
                public Stream<Emitable> emit(Output output) {
                    output.writer(EmittedEdge.class, encodedElement.label()).writeToOutput(this);
                    return stream();
                }
            };
        } else {
            throw new RuntimeException(encodedElement.getClass().getName() + " not supported by " + TinkerPopGraphEncoder.class.getSimpleName());
        }
    }

    @Override
    public void close() {
        try {
            graph.close();
        } catch (Exception e) {
            throw errorHandler.handleError(e);
        }
    }

    @Override
    public boolean skipEntry(Element line) {
        return false;
    }

    public Graph getGraph() {
        return graph;
    }

}
