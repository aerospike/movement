package com.aerospike.movement.tinkerpop.common.instrumentation;


import com.aerospike.movement.runtime.core.local.JVMGlobalRuntimeMetrics;
import com.aerospike.movement.util.core.ErrorUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static org.apache.commons.lang3.ArrayUtils.toMap;

/**
 * Will call pre-defined callbacks to handle various error types encountered
 */
public class ErrorHandledGraph extends InstrumentedGraph {
    protected final Map<Class<Exception>, Function<Exception, Void>> errorHandlers;
    protected final Function<Exception, Void> defaultHandler;
    private static final String FN_CTX = "fnCtx";
    private static final String ADD_VERTEX = "addVertex";
    private static final String VERTICES = "vertices";
    private static final String EDGES = "edges";
    private static final String VERTEX_IDS = "vertexIds";
    private static final String EDGE_IDS = "edgeIds";
    private static final String RETRY_COUNT = "retryCount";


    public ErrorHandledGraph(final Graph wrappedGraph, final Map<Class<Exception>, Function<Exception, Void>> errorHandlers) {
        super(wrappedGraph);
        this.errorHandlers = errorHandlers;
        this.defaultHandler = errorHandlers.get(Exception.class);
    }

    private Optional<Object> wrappedOperation(Callable<?> callable, Map<Object, Object> context) {
        try {
            return Optional.of(callable.call());
        } catch (Exception e) {
            JVMGlobalRuntimeMetrics.reportError(e);
            errorHandlers.getOrDefault(e.getClass(), defaultHandler).apply(e);
            return applyRetry(context);
        }
    }

    private Optional<Object> applyRetry(final Map<Object, Object> context) {
        context.computeIfAbsent(RETRY_COUNT, k -> new RetryCounter().increment());
        context.compute(RETRY_COUNT, (k, v) -> ((RetryCounter) v).increment());
        if ((Integer) context.get(RETRY_COUNT) > 3) {
            return Optional.empty();
        }
        switch ((String) context.get(FN_CTX)) {
            case ADD_VERTEX:
                return Optional.ofNullable(this.addVertex(addVertexContextToObjects(context)));
            case VERTICES:
                return Optional.ofNullable(this.vertices(context.get(VERTEX_IDS)));
            case EDGES:
                return Optional.ofNullable(this.edges(context.get(EDGE_IDS)));
            case "variables":
                return Optional.ofNullable(graph.variables());
            case "configuration":
                return Optional.ofNullable(graph.configuration());


            default:
                throw new RuntimeException("Unknown function context: " + context.get(FN_CTX));
        }
    }

    private String addVertexContextToObjects(Map<Object, Object> context) {
        throw ErrorUtil.unimplemented();
    }

    private static class RetryCounter {
        private int count = 0;

        public RetryCounter increment() {
            count++;
            return this;
        }

        public int getCount() {
            return count;
        }
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        if (keyValues.length != 0 && keyValues[0].getClass().equals(RetryCounter.class)) {
            RetryCounter counter = (RetryCounter) keyValues[0];
            counter.increment();
            if (counter.getCount() > 3) {
                throw new RuntimeException("Too many retries");
            }
        }
        final Object[] keyValuesWithoutFirstElement = new Object[keyValues.length - 1];
        System.arraycopy(keyValues, 1, keyValuesWithoutFirstElement, 0, keyValues.length - 1);

        final Map<Object, Object> x = toMap(keyValuesWithoutFirstElement);
        x.put(FN_CTX, ADD_VERTEX);
        return (Vertex) wrappedOperation(() -> graph.addVertex(keyValues), x).get();
    }

    private static Map.Entry<Object, Object[]> pop(Object[] packedArray) {
        final Object[] keyValuesWithoutFirstElement = new Object[packedArray.length - 1];
        System.arraycopy(packedArray, 1, keyValuesWithoutFirstElement, 0, packedArray.length - 1);
        Object firstObject = packedArray[0];
        return new AbstractMap.SimpleEntry<>(firstObject, keyValuesWithoutFirstElement);
    }

    private static Object[] addElementToFront(Object[] original, Object element) {
        Object[] newArray = new Object[original.length + 1];
        newArray[0] = element;
        System.arraycopy(original, 0, newArray, 1, original.length);
        return newArray;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        Map<Object, Object> x = new HashMap<>();
        x.put(VERTICES, VERTICES);
        x.put(VERTEX_IDS, vertexIds);
        return (Iterator<Vertex>) wrappedOperation(() -> {
                    if (vertexIds.length != 0 && vertexIds[0].getClass().equals(RetryCounter.class)) {
                        Map.Entry<Object, Object[]> counterHeadOrigArgsTail = pop(vertexIds);
                        RetryCounter counter = (RetryCounter) counterHeadOrigArgsTail.getKey();
                        counter.increment();
                        if (counter.getCount() > 3) {
                            throw ErrorUtil.unimplemented();
                        }
                        return graph.vertices(counterHeadOrigArgsTail.getValue());
                    }
                    return graph.vertices(vertexIds);
                },
                x).get();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        Map<Object, Object> x = new HashMap<>();
        x.put(FN_CTX, EDGES);
        x.put(EDGE_IDS, edgeIds);
        return (Iterator<Edge>) wrappedOperation(() -> graph.edges(edgeIds), x).get();
    }


    @Override
    public Variables variables() {
        HashMap<Object, Object> x = new HashMap<>() {{
            put(FN_CTX, "variables");
        }};
        return (Variables) wrappedOperation(() -> graph.variables(), x).get();
    }

    @Override
    public Configuration configuration() {
        HashMap<Object, Object> x = new HashMap<>() {{
            put(FN_CTX, "configuration");
        }};
        return (Configuration) wrappedOperation(() -> graph.configuration(), x).get();
    }
}
