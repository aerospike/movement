package com.aerospike.movement.tinkerpop.common.instrumentation;

import com.aerospike.movement.util.core.ErrorUtil;
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
        throw ErrorUtil.unimplemented();
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Object id() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public String label() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Graph graph() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        throw ErrorUtil.unimplemented();
    }
}
