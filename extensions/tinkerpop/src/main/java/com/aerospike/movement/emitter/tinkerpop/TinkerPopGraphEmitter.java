/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.runtime.tinkerpop.TinkerPopGraphDriver;
import com.aerospike.movement.structure.core.graph.TypedField;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.movement.emitter.tinkerpop.TinkerPopGraphEmitter.Config.Keys.PASSTHRU;

public class TinkerPopGraphEmitter extends Loadable implements Emitter, Emitter.SelfDriving, Emitter.Constrained {
    public static final AtomicBoolean initialized = new AtomicBoolean(false);
    public final TinkerPopGraphDriver driver;

    public static ConcurrentHashMap<String, List<TypedField>> vertexPropertyKeyCache = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, List<TypedField>> edgePropertyKeyCache = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, Semaphore> readLocks = new ConcurrentHashMap<>();
    public static final String GLOBAL = "GLOBAL";

    static {
        readLocks.put(GLOBAL, new Semaphore(1));
    }

    @Override
    public void init(final Configuration config) {
        vertexPropertyKeyCache.clear();
        edgePropertyKeyCache.clear();
//        driver.init(config);
    }

    @Override
    public WorkChunkDriver driver(final Configuration callerConfig) {
        return driver;
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
            return ConfigUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String GRAPH_PROVIDER = "emitter.tinkerpop.graph.provider";
            public static final String PASSTHRU = "passthru";

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, TinkerPopGraphDriver.class.getName());
            put(PASSTHRU, String.valueOf(true));
        }};
    }

    public static final Config CONFIG = new Config();
    private final Configuration config;
    private final Graph graph;


    private TinkerPopGraphEmitter(final Graph graph,
                                  final Configuration config) {
        super(Config.INSTANCE, config);
        this.graph = graph;
        this.config = config;
        this.driver = (TinkerPopGraphDriver) RuntimeUtil.lookupOrLoad(TinkerPopGraphDriver.class, config);
    }

    public static TinkerPopGraphEmitter open(final Configuration openConfig) {
        final Configuration config = ConfigUtil.withOverrides(openConfig, GraphProvider.Keys.CONTEXT, GraphProvider.Keys.INPUT);

        final Class providerClass = RuntimeUtil.loadClass(CONFIG.getOrDefault(Config.Keys.GRAPH_PROVIDER, config));
        final GraphProvider graphProvider = (GraphProvider) RuntimeUtil.openClass(providerClass, config);
        final Graph graph = graphProvider.getProvided(GraphProvider.GraphProviderContext.INPUT);
        return new TinkerPopGraphEmitter(graph, config);
    }


    @Override
    public Stream<Emitable> stream(final WorkChunkDriver workChunkDriver, final Runtime.PHASE phase) {
        if (!TinkerPopGraphDriver.class.isAssignableFrom(workChunkDriver.getClass()))
            throw RuntimeUtil.getErrorHandler(this).handleFatalError(new RuntimeException("TinkerPopGraphEmitter requires TinkerPopGraphDriver, another driver was provided: " + workChunkDriver.getClass()), workChunkDriver);
        final boolean passthrough = Boolean.parseBoolean((String) CONFIG.getOrDefault(PASSTHRU, config));

        Optional.ofNullable(workChunkDriver).orElseThrow(() -> RuntimeUtil.getErrorHandler(this)
                .handleFatalError(new RuntimeException("Work Chunk Driver is null"), phase));

        final Stream<List<?>> chunks = Stream.iterate(workChunkDriver.getNext(), Optional::isPresent, i -> workChunkDriver.getNext())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(wc ->
                        wc.stream().filter(Optional::isPresent)
                                .map(Optional::get)
                                .map(it -> it.unwrap())
                                .collect(Collectors.toList()));

        return chunks.flatMap(listOfIdsOrElements -> {
            try {
                if (phase.equals(Runtime.PHASE.ONE)) {
                    final List<Vertex> vertices = passthrough ?
                            (List<Vertex>) listOfIdsOrElements :
                            IteratorUtils.list(graph.vertices(listOfIdsOrElements.toArray()));
                    return vertices.stream().map(TinkerPopVertex::new);
                } else if (phase.equals(Runtime.PHASE.TWO)) {
                    final List<Edge> edges = passthrough ?
                            (List<Edge>) listOfIdsOrElements :
                            IteratorUtils.list(graph.edges(listOfIdsOrElements.toArray()));
                    return edges.stream().map(TinkerPopEdge::new);
                } else {
                    throw errorHandler.error("Unimplemented PHASE", phase);
                }
            } catch (Exception e) {
                throw RuntimeUtil.getErrorHandler(this).handleFatalError(e);
            }
        });
    }

    @Override
    public List<Runtime.PHASE> phases() {
        return List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO);
    }

    @Override
    public void onClose() {
        try {
            vertexPropertyKeyCache.clear();
            edgePropertyKeyCache.clear();
            graph.close();
        } catch (Exception e) {
            throw errorHandler.handleError(e, this);
        }
    }


    public List<TypedField> getAllPropertyKeysForVertexLabel(final String label) {
        final String lockName = "vertex:" + label;
        readLocks.computeIfAbsent(lockName, it -> new Semaphore(1));
        if (readLocks.get(lockName).availablePermits() == 0) {
            acquireReadLock(lockName);
            releaseReadLock(lockName);
        }
        return vertexPropertyKeyCache.computeIfAbsent(label, it -> {
            acquireReadLock(lockName);
            final List<String> results = graph.traversal().V()
                    .hasLabel(label)
                    .properties()
                    .key()
                    .dedup()
                    .toList();
            releaseReadLock(lockName);
            return sampleVertexTypes(label, results);
        });

    }

    private List<TypedField> sampleVertexTypes(String label, List<String> results) {
        return results.stream().map(field -> {
            final Object sample = graph.traversal().V().hasLabel(label).has(field).limit(1).properties(field).value().next();
            final boolean list = List.class.isAssignableFrom(sample.getClass());
            return new TypedField(field, list, sample.getClass());
        }).collect(Collectors.toList());
    }

    private List<TypedField> sampleEdgeTypes(String label, List<String> results) {
        return results.stream().map(field -> {
            final Object sample = graph.traversal().E().hasLabel(label).has(field).limit(1).properties(field).value().next();
            final boolean list = List.class.isAssignableFrom(sample.getClass());
            return new TypedField(field, list, sample.getClass());
        }).collect(Collectors.toList());
    }

    public List<TypedField> getAllPropertyKeysForEdgeLabel(final String label) {
        final String lockName = "edge:" + label;
        readLocks.computeIfAbsent(lockName, it -> new Semaphore(1));
        if (readLocks.get(lockName).availablePermits() == 0) {
            acquireReadLock(lockName);
            releaseReadLock(lockName);
        }
        return edgePropertyKeyCache.computeIfAbsent(label, it -> {
            acquireReadLock(lockName);
            final List<String> results = graph.traversal().V()
                    .hasLabel(label)
                    .properties()
                    .key()
                    .dedup()
                    .toList();
            releaseReadLock(lockName);
            return sampleEdgeTypes(label, results);
        });
    }

    public void acquireReadLock(final String name) {
        final Semaphore lock = readLocks.get(GLOBAL);
        try {
            lock.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void releaseReadLock(final String name) {
        readLocks.get(GLOBAL).release();
    }

}
