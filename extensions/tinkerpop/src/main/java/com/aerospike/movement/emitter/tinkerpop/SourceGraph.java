package com.aerospike.movement.emitter.tinkerpop;

import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.driver.WorkChunkId;
import com.aerospike.movement.util.core.iterator.OneShotSupplier;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class SourceGraph extends Loadable implements Emitter {
    public static final AtomicBoolean initialized = new AtomicBoolean(false);

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
            public static final String GRAPH_PROVIDER = "emitter.graphProvider";

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
        }};
    }

    public static final Config CONFIG = new Config();
    private final Optional<Iterator<Object>> idSupplier;
    private final Configuration config;
    private final Graph graph;
    private final Optional<GraphProvider> graphProvider;


    private SourceGraph(final Graph graph,
                        final Configuration config,
                        final Optional<GraphProvider> graphProvider,
                        final Optional<Iterator<Object>> idSupplier) {
        super(Config.INSTANCE, config);
        this.graph = graph;
        this.config = config;
        this.graphProvider = graphProvider;
        this.idSupplier = idSupplier;
    }

    public static SourceGraph open(final Configuration config) {
        final GraphProvider provider = (GraphProvider)
                RuntimeUtil.openClassRef(CONFIG.getOrDefault(Config.Keys.GRAPH_PROVIDER, config), config);
        final Graph graph = provider.getGraph();
        if (initialized.compareAndSet(false, true)) {
        }
        return new SourceGraph(graph, config, Optional.of(provider), Optional.empty());
    }


    @Override
    public Stream<Emitable> stream(final WorkChunkDriver workChunkDriver, final Runtime.PHASE phase) {
        final Stream<List<Object>> chunks = Stream.iterate(workChunkDriver.getNext(), Optional::isPresent, i -> workChunkDriver.getNext())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(wc -> IteratorUtils.list(IteratorUtils.map(wc, wcele -> wcele.getId())));
        return chunks.flatMap(listOfIds -> {
            if (phase.equals(Runtime.PHASE.ONE)) {
                return IteratorUtils.stream(graph.vertices(listOfIds.toArray())).map(TinkerPopVertex::new);
            } else if (phase.equals(Runtime.PHASE.TWO)) {
                return IteratorUtils.stream(graph.edges(listOfIds.toArray())).map(TinkerPopEdge::new);
            } else {
                throw errorHandler.error("Unknown object type ", listOfIds.getClass().getName());
            }
        });
    }

    @Override
    public List<Runtime.PHASE> phases() {
        return List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO);
    }

    @Override
    public void close() {
        try {
            graph.close();
        } catch (Exception e) {
            throw errorHandler.handleError(e, this);
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
}
