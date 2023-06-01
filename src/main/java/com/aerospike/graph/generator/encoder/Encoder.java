package com.aerospike.graph.generator.encoder;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;

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
