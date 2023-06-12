package com.aerospike.graph.generator.common.tinkerpop;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;

public abstract class InstrumentedGraph implements Graph {
    final Graph graph;

    public InstrumentedGraph(final Graph wrappedGraph) {
        this.graph = wrappedGraph;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return graph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return graph.compute();
    }

    @Override
    public Transaction tx() {
        return graph.tx();
    }

    @Override
    public void close() throws Exception {
        graph.close();
    }

    @Override
    public Variables variables() {
        return graph.variables();
    }

    @Override
    public Configuration configuration() {
        return graph.configuration();
    }

}
