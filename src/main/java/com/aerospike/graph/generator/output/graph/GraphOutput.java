package com.aerospike.graph.generator.output.graph;

import com.aerospike.graph.generator.common.tinkerpop.GraphProvider;
import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.emitter.generated.GeneratedVertex;
import com.aerospike.graph.generator.emitter.tinkerpop.SourceGraph;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.encoder.format.tinkerpop.GraphEncoder;
import com.aerospike.graph.generator.encoder.format.tinkerpop.TraversalEncoder;
import com.aerospike.graph.generator.output.Output;
import com.aerospike.graph.generator.output.OutputWriter;
import com.aerospike.graph.generator.runtime.CapturedError;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphOutput implements Output, OutputWriter {
    private final GraphEncoder encoder;
    private final AtomicLong vertexMetric;
    private final AtomicLong edgeMetric;


    public GraphOutput(final GraphEncoder encoder) {
        this.encoder =  encoder;
        this.vertexMetric = new AtomicLong(0);
        this.edgeMetric = new AtomicLong(0);
    }

    public static GraphOutput open(Configuration config) {
        return new GraphOutput((GraphEncoder) RuntimeUtil.loadEncoder(config));
    }

    @Override
    public Stream<Optional<CapturedError>> writeVertexStream(final Stream<EmittedVertex> vertexStream) {
        return vertexStream.map(it -> {
            Optional<CapturedError> result;
            try {
                this.writeVertex(it);
            } catch (Exception e) {
                result = Optional.of(new CapturedError(e, new GeneratedVertex.GeneratedVertexId(it.id().getId())));
            }
            result = Optional.empty();
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