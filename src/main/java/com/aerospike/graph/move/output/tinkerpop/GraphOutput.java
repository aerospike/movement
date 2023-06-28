package com.aerospike.graph.move.output.tinkerpop;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.encoding.format.tinkerpop.GraphEncoder;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.output.OutputWriter;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphOutput implements Output, OutputWriter {
    private final GraphEncoder encoder;
    private final AtomicLong vertexMetric;
    private final AtomicLong edgeMetric;


    public GraphOutput(final GraphEncoder encoder) {
        this.encoder = encoder;
        this.vertexMetric = new AtomicLong(0);
        this.edgeMetric = new AtomicLong(0);
    }

    public static GraphOutput open(Configuration config) {
        return new GraphOutput((GraphEncoder) RuntimeUtil.loadEncoder(config));
    }


    @Override
    public OutputWriter vertexWriter(final String label) {
        return this;
    }

    @Override
    public OutputWriter edgeWriter(final String label) {
        return this;
    }

    @Override
    public Long getEdgeMetric() {
        return edgeMetric.get();
    }

    @Override
    public Long getVertexMetric() {
        return vertexMetric.get();
    }

    @Override
    public void writeEdge(final Emitable edge) {
        encoder.encodeEdge((EmittedEdge) edge);
        edgeMetric.addAndGet(1);
    }

    @Override
    public void writeVertex(final Emitable vertex) {
        encoder.encodeVertex((EmittedVertex) vertex);
        vertexMetric.addAndGet(1);
    }

    @Override
    public void init() {
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        try {
            encoder.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropStorage() {
        encoder.getGraph().traversal().V().drop().iterate();
    }
}
