package com.aerospike.graph.generator.output.graph;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.emitter.generated.GeneratedVertex;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.encoder.format.tinkerpop.TraversalEncoder;
import com.aerospike.graph.generator.output.Output;
import com.aerospike.graph.generator.output.OutputWriter;
import com.aerospike.graph.generator.runtime.CapturedError;
import com.aerospike.graph.generator.util.RuntimeUtil;
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
                result = Optional.of(new CapturedError(e, new GeneratedVertex.GeneratedVertexId(it.id().getId())));
            }
            return result;
        });
    }

    @Override
    public Stream<Optional<CapturedError>> writeEdgeStream(final Stream<EmittedEdge> edgeStream) {
        return null;
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
    public void writeEdge(final EmittedEdge edge) {
        encoder.encodeEdge(edge);
        edgeMetric.addAndGet(1);
    }

    @Override
    public void writeVertex(final EmittedVertex vertex) {
        encoder.encodeVertex(vertex);
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
        System.out.printf("waited %d seconds to drop data \n", count);
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
