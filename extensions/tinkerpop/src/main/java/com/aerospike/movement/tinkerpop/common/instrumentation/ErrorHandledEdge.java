package com.aerospike.movement.tinkerpop.common.instrumentation;

import com.aerospike.movement.util.core.ErrorUtil;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;

public class ErrorHandledEdge implements Edge {
    @Override
    public Iterator<Vertex> vertices(Direction direction) {
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
    public <V> Property<V> property(String key, V value) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        throw ErrorUtil.unimplemented();
    }
}
