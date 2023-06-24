package com.aerospike.graph.move.output;

import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.util.CapturedError;

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
