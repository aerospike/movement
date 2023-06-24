package com.aerospike.graph.move.encoding;

import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class Encoder<O> {
    public abstract O encodeEdge(EmittedEdge edge);
    public abstract O encodeVertex(EmittedVertex vertex);
    public abstract O encodeVertexMetadata(EmittedVertex vertex);
    public abstract O encodeEdgeMetadata(EmittedEdge edge);
    public abstract O encodeVertexMetadata(String label);
    public abstract O encodeEdgeMetadata(String label);
    public abstract String getExtension();
    public abstract void close();
}
