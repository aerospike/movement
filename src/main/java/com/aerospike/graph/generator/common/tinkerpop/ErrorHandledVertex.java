package com.aerospike.graph.generator.common.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;

public class ErrorHandledVertex implements Vertex {
    private final Vertex vertex;
    private final ErrorHandledGraph ehGraph;

    public ErrorHandledVertex(final Vertex wrappedVertex, final ErrorHandledGraph errorHandledGraph){
        this.vertex = wrappedVertex;
        this.ehGraph = errorHandledGraph;
    }
    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        return null;
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return null;
    }

    @Override
    public Object id() {
        return null;
    }

    @Override
    public String label() {
        return null;
    }

    @Override
    public Graph graph() {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        return null;
    }
}
