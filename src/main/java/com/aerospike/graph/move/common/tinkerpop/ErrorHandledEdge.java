package com.aerospike.graph.move.common.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;

public class ErrorHandledEdge implements Edge {
    @Override
    public Iterator<Vertex> vertices(Direction direction) {
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
    public <V> Property<V> property(String key, V value) {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        return null;
    }
}
