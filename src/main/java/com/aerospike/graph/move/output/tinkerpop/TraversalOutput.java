package com.aerospike.graph.move.output.tinkerpop;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.encoding.format.tinkerpop.TraversalEncoder;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.output.OutputWriter;
import com.aerospike.graph.move.structure.EmittedIdImpl;
import com.aerospike.graph.move.util.CapturedError;
import com.aerospike.graph.move.util.ErrorUtil;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TraversalOutput implements Output, OutputWriter {
    private final Encoder<Element> encoder;
    private final AtomicLong vertexMetric;
    private final AtomicLong edgeMetric;
    Logger logger = LoggerFactory.getLogger(TraversalOutput.class);

    private TraversalOutput(final Encoder<Element> encoder) {
        this.encoder = encoder;
        this.vertexMetric = new AtomicLong(0);
        this.edgeMetric = new AtomicLong(0);
    }

    public static TraversalOutput open(Configuration config) {
        return new TraversalOutput(RuntimeUtil.loadEncoder(config));
    }

    @Override
    public Stream<Optional<CapturedError>> writeVertexStream(final Stream<EmittedVertex> vertexStream) {
        return vertexStream.map(it -> {
            Optional<CapturedError> result;
            try {
                this.writeVertex(it);
                result = Optional.empty();
            } catch (Exception e) {
                result = Optional.of(new CapturedError(e, new EmittedIdImpl(it.id().getId())));
            }
            return result;
        });
    }

    @Override
    public Stream<Optional<CapturedError>> writeEdgeStream(final Stream<EmittedEdge> edgeStream) {
        throw ErrorUtil.unimplemented();
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
        encoder.close();
    }

    @Override
    public void dropStorage() {
        GraphTraversalSource g = ((TraversalEncoder) encoder).getTraversal();
        g.V().drop().iterate();
        int count = 0;
        while (g.V().hasNext()) {
            count++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        GraphTraversalSource g = ((TraversalEncoder) encoder).getTraversal();
        Long verticesWritten = g.V().count().next();
        Long edgesWritten = g.E().count().next();
        StringBuilder sb = new StringBuilder();
        sb.append("TraversalOutput: \n");
        sb.append("\n  Vertices Written: ").append(verticesWritten).append("\n");
        sb.append("\n  Edges Written: ").append(edgesWritten).append("\n");
        return sb.toString();
    }
}
