/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common.instrumentation;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.tinkerpop.common.instrumentation.impl.CachedGraph;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;

public abstract class WrappedGraph extends Loadable implements Graph {


    public static class Keys {
        public static final String GRAPH_PROVIDER_IMPL = "graph.provider.impl";
    }


    public static class Methods {
        public static final String ADD_VERTEX = "addVertex";
        public static final String VERTICES = "vertices";
        public static final String EDGES = "edges";
        public static final String CLOSE = "close";
        public static final String VARIABLES = "variables";
        public static final String CONFIGURATION = "configuration";
    }


    private final Graph wrappedGraph;

    public WrappedGraph(final Graph graph, final ConfigurationBase configurationMeta, final Configuration config) {
        super(configurationMeta, config);
        this.wrappedGraph = graph;
    }

    public WrappedGraph(final Class<Graph> graphClass, final ConfigurationBase configurationMeta, final Configuration config) {
        this((Graph) RuntimeUtil.openClassRef(CachedGraph.Config.INSTANCE
                                .getOrDefault(CachedGraph.Config.Keys.GRAPH_PROVIDER, config),
                        config),
                configurationMeta, config);
    }


    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return wrappedGraph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return wrappedGraph.compute();
    }

    @Override
    public Transaction tx() {
        return wrappedGraph.tx();
    }

    @Override
    public void close() throws Exception {
        wrappedGraph.close();
    }

    @Override
    public Variables variables() {
        return wrappedGraph.variables();
    }

    @Override
    public Configuration configuration() {
        return wrappedGraph.configuration();
    }


}
