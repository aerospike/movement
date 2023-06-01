package com.aerospike.graph.generator.emitter;

import com.aerospike.graph.generator.emitter.generated.schema.def.VertexSchema;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Emitter {

    Stream<EmittedVertex> vertexStream();
    Stream<EmittedVertex> vertexStream(long startId, long endId);
    Stream<EmittedEdge> edgeStream();

    Emitter withIdSupplier(Iterator<Long> idSupplier);

    void close();
    List<String> getAllPropertyKeysForVertexLabel(String label);
    List<String> getAllPropertyKeysForEdgeLabel(String label);

    // For testing.
    VertexSchema getRootVertexSchema();
}
