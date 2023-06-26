package com.aerospike.graph.move.emitter;

import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import org.apache.commons.configuration2.Configuration;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Emitter {
    static void init(int value, Configuration config) {
    }

    Stream<Emitable> phaseOneStream();

    Stream<Emitable> phaseOneStream(long startId, long endId);

    Stream<Emitable> phaseTwoStream();

    Stream<Emitable> phaseTwoStream(long startId, long endId);

    Iterator<Object> phaseOneIterator();

    Iterator<Object> phaseTwoIterator();

    Emitter withIdSupplier(Iterator<Object> idSupplier);

    void close();

    List<String> getAllPropertyKeysForVertexLabel(String label);

    List<String> getAllPropertyKeysForEdgeLabel(String label);
}
