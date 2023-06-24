package com.aerospike.graph.move.emitter;

import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Emitter {
    Stream<EmittedVertex> phaseOneStream();

    Stream<EmittedVertex> phaseOneStream(long startId, long endId);

    Stream<EmittedEdge> phaseTwoStream();

    Emitter withIdSupplier(Iterator<List<?>> idSupplier);

    void close();

    List<String> getAllPropertyKeysForVertexLabel(String label);

    List<String> getAllPropertyKeysForEdgeLabel(String label);
    // For testing.
}
