package com.aerospike.graph.move.common.tinkerpop.instrumentation;

import com.aerospike.graph.move.common.tinkerpop.instrumentation.InstrumentedGraph;
import com.aerospike.graph.move.runtime.local.JVMGlobalRuntimeMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public class PerformanceInstrumentedGraph extends InstrumentedGraph {
    private final Graph graph;

    public PerformanceInstrumentedGraph(final Graph wrappedGraph) {
        super(wrappedGraph);
        this.graph = wrappedGraph;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        PerfUtil.PerfMesurement perfMesurement = PerfUtil.start();
        final Vertex x = graph.addVertex(keyValues);
        long runtime = perfMesurement.checkpoint();
        JVMGlobalRuntimeMetrics.reportVertexPerformance(runtime);
        return x;
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return graph.vertices(vertexIds); //@todo read not yet instrumented
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return graph.edges(edgeIds); //@todo read not yet instrumented
    }

    private static class PerfUtil {
        static PerfMesurement start(){
            return new PerfMesurement(System.nanoTime());
        }

        public static class PerfMesurement{
            public long start;
            private PerfMesurement(long start){
                this.start = start;
            }
            public long checkpoint(){
                return System.nanoTime() - start;
            }
        }
    }


}
