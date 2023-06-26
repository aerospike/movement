package com.aerospike.graph.move.encoding;

import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;
import org.apache.commons.configuration2.Configuration;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Encoder<O> {
    static void init(int value, Configuration config) {
    }

    O encodeEdge(EmittedEdge edge);
    O encodeVertex(EmittedVertex vertex);
    O encodeVertexMetadata(EmittedVertex vertex);
    O encodeEdgeMetadata(EmittedEdge edge);
    O encodeVertexMetadata(String label);
    O encodeEdgeMetadata(String label);
    String getExtension();
    void close();
}
