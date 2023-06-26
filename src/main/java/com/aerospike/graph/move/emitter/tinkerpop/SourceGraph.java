package com.aerospike.graph.move.emitter.tinkerpop;

import com.aerospike.graph.move.common.tinkerpop.GraphProvider;
import com.aerospike.graph.move.emitter.*;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.structure.EmittedId;
import com.aerospike.graph.move.structure.EmittedIdImpl;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.ErrorUtil;
import com.aerospike.graph.move.util.MovementIteratorUtils;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class SourceGraph extends Emitter.PhasedEmitter {
    public static final Config CONFIG = new Config();
    private final Optional<Iterator<Object>> idSupplier;
    private final String batchSize;
    private final Configuration config;

    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String GRAPH_PROVIDER = "emitter.graphProvider";
            public static final String BATCH_SIZE = "emitter.batchSize";

        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.BATCH_SIZE, "1000");
        }};
    }

    public static SourceGraph open(Configuration config) {
        final GraphProvider provider = (GraphProvider)
                RuntimeUtil.openClassRef(CONFIG.getOrDefault(config, Config.Keys.GRAPH_PROVIDER), config);
        return new SourceGraph(provider.getGraph(), config, Optional.of(provider), Optional.empty());
    }

    private final Graph graph;
    private final Optional<GraphProvider> graphProvider;


    private SourceGraph(final Graph graph,
                        final Configuration config,
                        final Optional<GraphProvider> graphProvider,
                        final Optional<Iterator<Object>> idSupplier) {
        this.graph = graph;
        this.config = config;
        this.graphProvider = graphProvider;
        this.idSupplier = idSupplier;
        this.batchSize = CONFIG.getOrDefault(config, Config.Keys.BATCH_SIZE);
    }


    public Stream<Emitable> stream(Stream<Object> inputStream, Runtime.PHASE phase) {
        return stream(inputStream.iterator(), phase);
    }

    @Override
    public Stream<Emitable> stream(Iterator<Object> iterator, Runtime.PHASE phase) {
        return IteratorUtils.stream(iterator)
                .flatMap(object -> {
                    Optional.ofNullable(object).orElseThrow(() -> new RuntimeException("Null object"));
                    if(List.class.isAssignableFrom(object.getClass())) {
                        return IteratorUtils.stream(graph.vertices(((List<Object>)object).toArray())).map(TinkerPopVertex::new);
                    }else if (Vertex.class.isAssignableFrom(object.getClass())){
                        return Stream.of(new TinkerPopVertex((Vertex)object));
                    }else if (Edge.class.isAssignableFrom(object.getClass())){
                        return Stream.of(new TinkerPopEdge((Edge)object));
                    }else if (phase.equals(Runtime.PHASE.ONE)){
                        return IteratorUtils.stream(graph.vertices(object)).map(TinkerPopVertex::new);
                    }else if (phase.equals(Runtime.PHASE.TWO)){
                        return IteratorUtils.stream(graph.edges(object)).map(TinkerPopEdge::new);
                    }else {
                        throw new RuntimeException("Unknown object type " + object.getClass().getName());
                    }
                });
    }


    @Override
    public Stream<Emitable> stream(Runtime.PHASE phase) {
        return stream(IteratorUtils.stream(getDriverForPhase(phase)).flatMap(it -> it.stream()), phase);
    }

    @Override
    public Iterator<List<Object>> getDriverForPhase(Runtime.PHASE phase) {
        if (phase.equals(Runtime.PHASE.ONE)) {
            return getOrCreateDriverIterator(phase, (x) -> IteratorUtils.map(graph.vertices(), v -> v.id()));
        } else if (phase.equals(Runtime.PHASE.TWO)) {
            return getOrCreateDriverIterator(phase, (x) -> IteratorUtils.map(graph.edges(), e -> e.id()));
        }
        throw new IllegalStateException("Unknown phase " + phase);
    }

    @Override
    public void close() {
        try {
            graph.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getAllPropertyKeysForVertexLabel(final String label) {
        if (graphProvider.isPresent()) {
            return graphProvider.get().getAllPropertyKeysForVertexLabel(label);
        } else return graph.traversal().V()
                .hasLabel(label)
                .properties()
                .key()
                .dedup()
                .toList();
    }

    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        if (graphProvider.isPresent()) {
            return graphProvider.get().getAllPropertyKeysForEdgeLabel(label);
        }
        return graph.traversal().E()
                .hasLabel(label)
                .properties()
                .key()
                .dedup()
                .toList();
    }

    public class TinkerPopVertex implements EmittedVertex {
        private final Vertex vertex;

        public TinkerPopVertex(final Vertex vertex) {
            this.vertex = vertex;
        }

        @Override
        public Stream<Emitable> emit(final Output writer) {
            writer.vertexWriter(vertex.label()).writeVertex(this);
            return Stream.empty();
        }

        @Override
        public Stream<String> propertyNames() {
            return IteratorUtils.stream(vertex.properties()).map(p -> p.key());
        }

        @Override
        public Optional<Object> propertyValue(final String field) {
            if (field.equals("~id"))
                return Optional.of(vertex.id());
            if (field.equals("~label"))
                return Optional.of(vertex.label());
            //@todo multi properties
            if (!vertex.properties(field).hasNext() || !vertex.properties(field).next().isPresent()) {
                return Optional.empty();
            }
            return Optional.of(vertex.properties(field).next().value());
        }

        @Override
        public String label() {
            return vertex.label();
        }

        @Override
        public Stream<Emitable> stream() {
            return Stream.empty();
        }

        @Override
        public EmittedId id() {
            return new EmittedIdImpl(Long.valueOf(vertex.id().toString()));
        }
    }

    public class TinkerPopEdge implements EmittedEdge {
        private final Edge edge;

        public TinkerPopEdge(final Edge edge) {
            this.edge = edge;
        }

        @Override
        public Stream<Emitable> emit(final Output writer) {
            writer.edgeWriter(edge.label()).writeEdge(this);
            return Stream.empty();
        }

        @Override
        public EmittedId fromId() {
            return new EmittedIdImpl(Long.valueOf(edge.outVertex().id().toString()));
        }

        @Override
        public EmittedId toId() {
            return new EmittedIdImpl(Long.valueOf(edge.inVertex().id().toString()));
        }

        @Override
        public Stream<String> propertyNames() {
            return IteratorUtils.stream(edge.properties()).map(Property::key);
        }

        @Override
        public Optional<Object> propertyValue(final String name) {
            if (!edge.property(name).isPresent()) {
                return Optional.empty();
            }
            return Optional.of(edge.property(name).value());
        }

        @Override
        public String label() {
            return edge.label();
        }

        @Override
        public Stream<Emitable> stream() {
            return Stream.empty();
        }
    }

}
