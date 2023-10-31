/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.tinkerpop;

import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.tinkerpop.common.TinkerPopGraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.tinkerpop.Args;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinQueryParser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class TinkerPopGraphEmitter extends Loadable implements Emitter, Emitter.SelfDriving, Emitter.Constrained {
    public static final AtomicBoolean initialized = new AtomicBoolean(false);

    @Override
    public void init(final Configuration config) {

    }

    @Override
    public WorkChunkDriver driver(final Configuration configuration) {
        return null;
    }

    @Override
    public List<String> getConstraints(final Configuration callerConfig) {
        return List.of();
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
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, SuppliedWorkChunkDriver.class.getName());
        }};
    }

    public static final Config CONFIG = new Config();
    private final Optional<Iterator<Object>> idSupplier;
    private final Configuration config;
    private final Graph graph;


    private TinkerPopGraphEmitter(final Graph graph,
                                  final Configuration config,
                                  final Optional<Iterator<Object>> idSupplier) {
        super(Config.INSTANCE, config);
        this.graph = graph;
        this.config = config;
        this.idSupplier = idSupplier;
    }

    public static TinkerPopGraphEmitter open(final Configuration config) {
        final Class providerClass = RuntimeUtil.loadClass(CONFIG.getOrDefault(Config.Keys.GRAPH_PROVIDER, config));
        Object x = RuntimeUtil.openClass(providerClass, config);
        if (Graph.class.isAssignableFrom(x.getClass()))
            return new TinkerPopGraphEmitter((Graph) x, config, Optional.empty());
        else if (GraphProvider.class.isAssignableFrom(x.getClass()))
            return new TinkerPopGraphEmitter(((GraphProvider) x).getGraph(), config, Optional.empty());
        else throw new RuntimeException(x.getClass() + " is not a supported Graph provider");
    }


    @Override
    public Stream<Emitable> stream(final WorkChunkDriver workChunkDriver, final Runtime.PHASE phase) {
        Optional.ofNullable(workChunkDriver).orElseThrow(() -> RuntimeUtil.getErrorHandler(this)
                .handleFatalError(new RuntimeException("Work Chunk Driver is null"), phase));
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

        return graph.traversal().V()
                .hasLabel(label)
                .properties()
                .key()
                .dedup()
                .toList();
    }

    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return graph.traversal().E()
                .hasLabel(label)
                .properties()
                .key()
                .dedup()
                .toList();
    }
}
