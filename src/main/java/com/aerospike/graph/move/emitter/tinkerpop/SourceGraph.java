package com.aerospike.graph.move.emitter.tinkerpop;

import com.aerospike.graph.move.common.tinkerpop.GraphProvider;
import com.aerospike.graph.move.emitter.*;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.structure.EmittedId;
import com.aerospike.graph.move.structure.EmittedIdImpl;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.ErrorUtil;
import com.aerospike.graph.move.util.MovementIteratorUtils;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class SourceGraph implements Emitter {
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

    @Override
    public Stream<Emitable> phaseOneStream() {
        if (!idSupplier.isPresent())
            return IteratorUtils.stream(graph.vertices()).map(TinkerPopVertex::new);
        return IteratorUtils.stream(MovementIteratorUtils.BatchIterator.create(idSupplier.get(), 1000))
                .flatMap(idBatch -> {
                    Object[] ids = idBatch.stream().toArray();
                    return IteratorUtils.stream(graph.vertices(ids)).map(TinkerPopVertex::new);
                });
    }

    @Override
    public Stream<Emitable> phaseOneStream(final long startId, final long endId) {
        Object[] ids = LongStream.range(startId, endId).boxed().toArray();
        return IteratorUtils.stream(graph.vertices(ids)).map(TinkerPopVertex::new);
    }

    @Override
    public Stream<Emitable> phaseTwoStream() {
        return IteratorUtils.stream(graph.edges()).map(TinkerPopEdge::new);
    }

    @Override
    public Stream<Emitable> phaseTwoStream(long startId, long endId) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Iterator<Object> phaseOneIterator() {
        return IteratorUtils.of(phaseOneStream());
    }

    @Override
    public Iterator<Object> phaseTwoIterator() {
        return IteratorUtils.of(phaseTwoStream());
    }

    @Override
    public Emitter withIdSupplier(Iterator<Object> idSupplier) {
        //@todo ignored
        return this;
    }

    @Override
    public void close() {
        try {
            graph.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
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

    @Override
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
