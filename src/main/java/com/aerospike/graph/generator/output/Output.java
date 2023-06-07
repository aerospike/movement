package com.aerospike.graph.generator.output;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.runtime.CapturedError;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Output {
    Stream<Optional<CapturedError>> writeVertexStream(Stream<EmittedVertex> vertexStream);

    Stream<Optional<CapturedError>> writeEdgeStream(Stream<EmittedEdge> edgeStream);

    OutputWriter vertexWriter(String label);

    OutputWriter edgeWriter(String label);
    Long getEdgeMetric();
    Long getVertexMetric();
    void close();
    void dropStorage();

}
