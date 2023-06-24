package com.aerospike.graph.move;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class VirtualVertexPlugin {
    public static Vertex open(final Graph graph) {
        return SystemVertex.getInstance(graph);
    }


    private static class JobVertex extends VirtualVertexPlugin.VirtualVertex {

        private final String id;

        public JobVertex(Graph graph) {
            super(graph);
            this.id = UUID.randomUUID().toString();
        }

        @Override
        public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
            return null;
        }

        @Override
        public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
            return Collections.emptyIterator();
        }

        @Override
        public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
            return Collections.emptyIterator();
        }

        @Override
        public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
            return null;
        }

        @Override
        public Object id() {
            return id;
        }

        @Override
        public String label() {
            return "Job";
        }

        @Override
        public void remove() {

        }
    }
    private static class SystemVertex extends VirtualVertexPlugin.VirtualVertex {
        private static final AtomicReference<SystemVertex> INSTANCE = new AtomicReference<>();

        private SystemVertex(Graph graph) {
            super(graph);
        }
        public static SystemVertex getInstance(final Graph graph){
            return INSTANCE.compareAndExchange(null, new SystemVertex(graph));
        }
        @Override
        public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
            if (!validateVertexType(inVertex))
                throw new UnsupportedOperationException("cannot add vertex of type" + inVertex.label());
            if (!validateKeyValues(inVertex, keyValues))
                throw new UnsupportedOperationException("keys or value types unsupported for vertex of type " + inVertex.label());
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
        public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
            return null;
        }

        @Override
        public Object id() {
            return 0L;
        }

        @Override
        public String label() {
            return "system";
        }

        @Override
        public void remove() {

        }
    }

    private static abstract class
    VirtualVertex implements Vertex {
        private final Graph graph;

        public VirtualVertex(Graph graph){
            this.graph = graph;
        }
        protected boolean validateKeyValues(Vertex inVertex, Object[] keyValues) {
            return true;
        }

        protected boolean validateVertexType(Vertex inVertex) {
            return true;
        }

        @Override
        public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
            return null;
        }



        @Override
        public Graph graph() {
            return graph;
        }

    }
}
