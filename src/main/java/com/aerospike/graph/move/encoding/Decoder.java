package com.aerospike.graph.move.encoding;

import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;

public abstract class Decoder<O> {
    public abstract O decodeEdge(EmittedEdge edge);

    public abstract O decodeVertex(EmittedVertex vertex);

    public abstract O decodeVertexMetadata(EmittedVertex vertex);

    public abstract O decodeEdgeMetadata(EmittedEdge edge);

    public abstract O decodeVertexMetadata(String label);

    public abstract O decodeEdgeMetadata(String label);

    public abstract String getExtension();

    public abstract void close();


}
